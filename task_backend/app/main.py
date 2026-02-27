from pathlib import Path
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from app.core.settings import settings
from pymongo import MongoClient
import logging
import asyncio

# Import MySQL integration services
from app.services.sql_sync_service import sync_service
from app.services.backup_sync_service import backup_sync_service

# Configure essential logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
    ]
)
logger = logging.getLogger(__name__)

# Set specific loggers to INFO/ERROR to reduce noise
logging.getLogger("app").setLevel(logging.INFO)
logging.getLogger("app.services.stage_tracking_service").setLevel(logging.DEBUG)
logging.getLogger("app.services.stage_assignment_service").setLevel(logging.DEBUG)
logging.getLogger("app.api.v1.routers.tasks").setLevel(logging.DEBUG)
logging.getLogger("app.api.v1.routers.employee_assignment").setLevel(logging.DEBUG)
logging.getLogger("app.api.v1.routers.stage_tracking").setLevel(logging.DEBUG)
logging.getLogger("uvicorn").setLevel(logging.WARNING)
logging.getLogger("uvicorn.access").setLevel(logging.WARNING)
logging.getLogger("fastapi").setLevel(logging.WARNING)
logging.getLogger("pymongo").setLevel(logging.WARNING)
logging.getLogger("pymongo.serverSelection").setLevel(logging.WARNING)
logging.getLogger("pymongo.connection").setLevel(logging.WARNING)
logging.getLogger("pymongo.command").setLevel(logging.WARNING)
logging.getLogger("pymongo.topology").setLevel(logging.WARNING)

app = FastAPI(title=settings.app_name, version="1.0.0")

# CORS middleware - Allow frontend origins
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        # Development origins
        "http://localhost:5018",
        "http://127.0.0.1:5018",
        "http://localhost:3000",
        "http://127.0.0.1:3000",
        "http://localhost:5173",
        "http://127.0.0.1:5173",
        "http://localhost:8080",
        "http://127.0.0.1:8080",
        # Production origins (uncomment and update for production)
        # "https://yourdomain.com",
        # "https://www.yourdomain.com",
        # "https://api.yourdomain.com",
    ],
    allow_credentials=True,
    allow_methods=["GET", "POST", "PUT", "DELETE", "OPTIONS", "PATCH"],
    allow_headers=["*"],
    expose_headers=["*"]
)

# Create uploads directory
Path(settings.uploads_dir).mkdir(parents=True, exist_ok=True)

# MongoDB initialization and connection check
@app.on_event("startup")
async def startup_event():
    """Initialize MongoDB connection on startup"""
    try:
        logger.info("="*60)
        logger.info("🚀 STARTING TASK ASSIGNMENT SYSTEM")
        logger.info("="*60)
        logger.info(f"📊 Connecting to MongoDB...")
        logger.info(f"   URI: {settings.mongodb_uri[:30]}...")
        logger.info(f"   Database: {settings.mongodb_db}")
        
        # Test MongoDB connection
        client = MongoClient(settings.mongodb_uri, serverSelectionTimeoutMS=5000)
        client.admin.command('ping')
        db = client[settings.mongodb_db]
        
        # Get collection counts
        employee_count = db.employee.count_documents({})
        tasks_count = db.tasks.count_documents({})
        profile_count = db.profile_building.count_documents({})
        permit_count = db.permit_files.count_documents({})
        
        logger.info("✅ MongoDB connection successful!")
        logger.info(f"📋 Collections status:")
        logger.info(f"   • employee: {employee_count} documents")
        logger.info(f"   • tasks: {tasks_count} documents")
        logger.info(f"   • profile_building: {profile_count} documents")
        logger.info(f"   • permit_files: {permit_count} documents")
        
        # Check embeddings
        with_embeddings = db.employee.count_documents({'embedding': {'$exists': True, '$ne': []}})
        logger.info(f"🎯 Embeddings: {with_embeddings}/{employee_count} employees")
        
        # Start MongoDB to ClickHouse sync service
        from app.services.sync_service import SyncService
        sync_service = SyncService()

        # Capture the main event loop for thread-safe async dispatch (SLA emissions, WebSocket-safe patterns)
        from app.services.clickhouse_service import clickhouse_service
        clickhouse_service.set_main_event_loop(asyncio.get_running_loop())
        
        # Initialize MySQL integration
        logger.info("🔗 Initializing MySQL integration...")
        try:
            await sql_sync_service.initialize()
            logger.info("✅ MySQL sync service initialized")
            
            # Test MySQL connection
            if sql_sync_service.mysql_service.test_mysql_connection():
                logger.info("✅ MySQL connection successful")
                
                # Periodic sync disabled - only manual sync available
                logger.info("ℹ️  Periodic employee sync disabled (manual only)")
                # asyncio.create_task(backup_sync_service.start_periodic_sync())
                # logger.info("✅ Backup sync service started")
            else:
                logger.error("❌ MySQL connection failed")
                
        except Exception as e:
            logger.error(f"❌ MySQL integration failed: {e}")
            logger.warning("⚠️  Continuing without MySQL integration")
        
        # Start sync worker in background
        asyncio.create_task(sync_service.start_sync_worker())
        logger.info("✅ Started MongoDB to ClickHouse sync service")
        
        # Start SLA event emitter for WebSocket notifications
        from app.services.sla_event_emitter import get_sla_emitter
        sla_emitter = get_sla_emitter()
        await sla_emitter.start()
        logger.info("✅ Started SLA event emitter")
        
        # Initial sync for recent data
        await sync_service.sync_recent_data()
        logger.info("📊 Completed initial MongoDB to ClickHouse sync")
        
        logger.info("="*60)
        logger.info("✅ BACKEND READY - MongoDB + ClickHouse Mode")
        logger.info("="*60)
        
        client.close()
    except Exception as e:
        logger.error("="*60)
        logger.error("❌ MONGODB CONNECTION FAILED!")
        logger.error(f"   Error: {str(e)}")
        logger.error("="*60)
        raise

@app.on_event("shutdown")
async def shutdown_event():
    """Graceful shutdown"""
    try:
        # Stop SLA event emitter
        from app.services.sla_event_emitter import get_sla_emitter
        sla_emitter = get_sla_emitter()
        await sla_emitter.stop()
        logger.info("✅ Stopped SLA event emitter")
    except Exception as e:
        logger.error(f"Error during shutdown: {e}")

# Import and include routers - MongoDB based
from app.api.v1.routers.employees import router as employees_router
from app.api.v1.routers.permit_files import router as permit_files_router
from app.api.v1.routers.tasks import router as tasks_router
from app.api.v1.routers.employee_tasks import router as employee_tasks_router
from app.api.v1.routers.gemini_recommendations import router as gemini_recommendations_router
from app.api.v1.routers.stage_tracking import router as stage_tracking_router
from app.api.v1.routers.employee_assignment import router as employee_assignment_router
from app.api.v1.routers.permit_reports import router as permit_reports_router
from app.api.v1.routers.analytics import router as analytics_router
from app.api.v1.routers.zip_assign import router as zip_assign_router
from app.api.v1.routers.file_lifecycle import router as file_lifecycle_router
# Add webhook router for SQL integration
from app.api.v1.routers.webhooks import router as webhooks_router
# Add MySQL admin router
from app.api.v1.routers.mysql_admin import router as mysql_admin_router

# Add automation router (now integrated with existing flow)
from app.api.v1.routers.automation import router as automation_router
# Add temporal automation router - commented out temporarily
# from app.api.v1.routers.temporal_automation import router as temporal_automation_router
# from app.api.v1.routers.temporal_integration import router as temporal_integration_router
# Add WebSocket router for real-time notifications
from app.api.v1.routers.websockets import router as websockets_router
from app.api.v1.routers.notifications import router as notifications_router
from app.api.v1.routers.websocket_events import websocket_endpoint, event_stream, websocket_manager
from app.api.v1.routers.frontend_compat import router as frontend_compat_router
from app.api.v1.routers.stage_configs import router as stage_configs_router

# Temporal routers commented out temporarily
# logger.info("🔥 Including temporal_integration router...")
# app.include_router(temporal_integration_router, prefix="/api/v1")

# Need to add this import
from app.services.sql_sync_service import SQLToMongoSyncService
sql_sync_service = SQLToMongoSyncService()

logger.info("🔥 Including websockets router...")
app.include_router(websockets_router, prefix="/api/v1")
logger.info("🔥 Including notifications router...")
app.include_router(notifications_router, prefix="/api/v1")

# Add WebSocket endpoint
logger.info("🔥 Including WebSocket endpoint...")
app.websocket("/ws/{employee_code}")(websocket_endpoint)

# Add SSE endpoint
logger.info("🔥 Including SSE endpoint...")
app.get("/api/v1/events/stream")(event_stream)
logger.info("🔥 Including ZIP-based assignment router...")
app.include_router(zip_assign_router, prefix="/api/v1")
logger.info("🔥 Including permit files router...")
app.include_router(permit_files_router, prefix="/api/v1")
logger.info("🔥 Including file lifecycle router...")
app.include_router(file_lifecycle_router, prefix="/api/v1")
logger.info("🔥 Including tasks router...")
app.include_router(tasks_router, prefix="/api/v1")
logger.info("🔥 Including employee assignment router...")
app.include_router(employee_assignment_router, prefix="/api/v1/employees")
logger.info("🔥 Including employees router...")
app.include_router(employees_router, prefix="/api/v1")
logger.info("🔥 Including employee tasks router...")
app.include_router(employee_tasks_router, prefix="/api/v1")
logger.info("🔥 Including gemini recommendations router...")
app.include_router(gemini_recommendations_router, prefix="/api/v1")
logger.info("🔥 Including stage tracking router...")
app.include_router(stage_tracking_router, prefix="/api/v1")
logger.info("🔥 Including permit reports router...")
app.include_router(permit_reports_router, prefix="/api/v1")
logger.info("🔥 Including automation router...")
app.include_router(automation_router, prefix="/api/v1")
logger.info("🔥 Including analytics router (ClickHouse)...")
app.include_router(analytics_router, prefix="/api/v1")
logger.info("🔥 Including webhook router for SQL integration...")
app.include_router(webhooks_router, prefix="/api/v1")
logger.info("🔥 Including MySQL admin router...")
app.include_router(mysql_admin_router, prefix="/api/v1")
logger.info("🔥 Including frontend compatibility router...")
app.include_router(frontend_compat_router, prefix="/api/v1")
logger.info("🔥 Including stage configs router...")
app.include_router(stage_configs_router, prefix="/api/v1")
# Temporal routers commented out temporarily
# logger.info("🔥 Including temporal automation router...")
# app.include_router(temporal_automation_router, prefix="/api/v1")
# logger.info("🔥 Including temporal_integration router...")
# app.include_router(temporal_integration_router, prefix="/api/v1")

logger.info("✅ All routers included successfully!")

@app.get("/")
async def root():
    return {"message": "Task Assignment System API", "version": "1.0.0", "mode": "mongodb", "database": settings.mongodb_db}

@app.get("/health")
async def health_check():
    return {"status": "healthy", "mode": "mongodb", "database": settings.mongodb_db}

@app.get("/api/v1/cors-test")
async def cors_test():
    """Test endpoint to verify CORS is working"""
    return {"message": "CORS test successful", "timestamp": "2026-02-20T10:00:00Z"}