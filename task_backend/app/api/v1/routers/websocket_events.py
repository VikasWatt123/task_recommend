"""
WebSocket and Server-Sent Events for real-time updates
"""
import json
import asyncio
from typing import Dict, Set
from fastapi import WebSocket, WebSocketDisconnect, HTTPException
from fastapi.responses import StreamingResponse
from app.db.mongodb import get_db
import logging

logger = logging.getLogger(__name__)

# Active WebSocket connections
active_connections: Dict[str, WebSocket] = {}

class WebSocketManager:
    """Manages WebSocket connections for real-time updates"""
    
    def __init__(self):
        self.active_connections: Dict[str, WebSocket] = {}
    
    async def connect(self, websocket: WebSocket, employee_code: str):
        """Accept and store WebSocket connection"""
        await websocket.accept()
        self.active_connections[employee_code] = websocket
        logger.info(f"WebSocket connected for employee {employee_code}")
        
        # Broadcast new connection
        await self.broadcast({
            "type": "employee_connected",
            "employee_code": employee_code,
            "timestamp": asyncio.get_event_loop().time()
        }, exclude_employee=employee_code)
    
    async def disconnect(self, employee_code: str):
        """Remove WebSocket connection"""
        if employee_code in self.active_connections:
            del self.active_connections[employee_code]
            logger.info(f"WebSocket disconnected for employee {employee_code}")
            
            # Broadcast disconnection
            await self.broadcast({
                "type": "employee_disconnected",
                "employee_code": employee_code,
                "timestamp": asyncio.get_event_loop().time()
            })
    
    async def send_personal_message(self, employee_code: str, message: dict):
        """Send message to specific employee"""
        if employee_code in self.active_connections:
            try:
                await self.active_connections[employee_code].send_text(json.dumps(message))
            except Exception as e:
                logger.error(f"Failed to send message to {employee_code}: {e}")
    
    async def broadcast(self, message: dict, exclude_employee: str = None):
        """Broadcast message to all connected employees"""
        disconnected = []
        for emp_code, websocket in self.active_connections.items():
            if emp_code != exclude_employee:
                try:
                    await websocket.send_text(json.dumps(message))
                except Exception as e:
                    logger.error(f"Failed to broadcast to {emp_code}: {e}")
                    disconnected.append(emp_code)
        
        # Clean up disconnected connections
        for emp_code in disconnected:
            await self.disconnect(emp_code)
    
    async def broadcast_task_update(self, task_data: dict):
        """Broadcast task assignment/update"""
        message = {
            "type": "task_update",
            "data": task_data,
            "timestamp": asyncio.get_event_loop().time()
        }
        await self.broadcast(message)
    
    async def broadcast_one_way_update(self, event_type: str, data: dict):
        """Broadcast one-way update (also sent via SSE)"""
        message = {
            "type": event_type,
            "data": data,
            "timestamp": asyncio.get_event_loop().time()
        }
        await self.broadcast(message)
    
    async def broadcast_employee_status(self, employee_code: str, status: str):
        """Broadcast employee status change"""
        message = {
            "type": "employee_status_update",
            "employee_code": employee_code,
            "status": status,
            "timestamp": asyncio.get_event_loop().time()
        }
        await self.broadcast(message)
    
    async def broadcast_sla_breach(self, breach_data: dict):
        """Broadcast SLA breach alert"""
        message = {
            "type": "sla_breach",
            "data": breach_data,
            "timestamp": asyncio.get_event_loop().time()
        }
        await self.broadcast(message)

# Global WebSocket manager
websocket_manager = WebSocketManager()

# WebSocket endpoint
async def websocket_endpoint(websocket: WebSocket, employee_code: str = None, token: str = None):
    """WebSocket endpoint for real-time communication"""
    if not employee_code:
        await websocket.close(code=4000, reason="Employee code required")
        return
    
    # Optional: Validate token here
    # if token and not validate_token(token):
    #     await websocket.close(code=4001, reason="Invalid token")
    #     return
    
    await websocket_manager.connect(websocket, employee_code)
    
    try:
        while True:
            # Receive messages from client
            data = await websocket.receive_text()
            try:
                message = json.loads(data)
                
                # Handle different message types
                if message.get("type") == "status_update":
                    await websocket_manager.broadcast_employee_status(
                        employee_code, 
                        message.get("status", "online")
                    )
                elif message.get("type") == "ping":
                    await websocket.send_text(json.dumps({"type": "pong"}))
                    
            except json.JSONDecodeError:
                logger.warning(f"Invalid JSON received from {employee_code}")
            except Exception as e:
                logger.error(f"Error processing message from {employee_code}: {e}")
                
    except WebSocketDisconnect:
        await websocket_manager.disconnect(employee_code)
    except Exception as e:
        logger.error(f"WebSocket error for {employee_code}: {e}")
        await websocket_manager.disconnect(employee_code)

# Server-Sent Events endpoint
async def event_stream():
    """SSE endpoint for one-way real-time updates"""
    async def event_generator():
        db = get_db()
        
        # Watch for changes in MongoDB
        try:
            while True:
                # Check for recent updates (last 5 seconds)
                now = asyncio.get_event_loop().time()
                
                # Check for new tasks
                new_tasks = list(db.tasks.find({
                    "assigned_at": {
                        "$gte": now - 5  # Last 5 seconds
                    }
                }).limit(10))
                
                for task in new_tasks:
                    yield f"event: task_assigned\ndata: {json.dumps({
                        'type': 'task_assigned',
                        'task_id': task.get('task_id'),
                        'assigned_to': task.get('assigned_to'),
                        'stage': task.get('stage'),
                        'timestamp': task.get('assigned_at')
                    }, default=str)}\n\n"
                
                # Check for employee status changes
                status_updates = list(db.employee.find({
                    "metadata.updated_at": {
                        "$gte": now - 5
                    }
                }).limit(10))
                
                for emp in status_updates:
                    yield f"event: employee_status\ndata: {json.dumps({
                        'type': 'status_updated',
                        'employee_code': emp.get('employee_code'),
                        'status': emp.get('employment', {}).get('status_1', 'unknown'),
                        'timestamp': emp.get('metadata', {}).get('updated_at')
                    }, default=str)}\n\n"
                
                # Check for SLA breaches
                from datetime import datetime, timedelta
                breach_threshold = datetime.utcnow() - timedelta(hours=24)
                breaches = list(db.tasks.find({
                    "status": {"$ne": "COMPLETED"},
                    "assigned_at": {"$lt": breach_threshold},
                    "stage": {"$exists": True}
                }).limit(5))
                
                for breach in breaches:
                    yield f"event: sla_breach\ndata: {json.dumps({
                        'type': 'sla_breach',
                        'task_id': breach.get('task_id'),
                        'employee_code': breach.get('assigned_to'),
                        'stage': breach.get('stage'),
                        'hours_overdue': (datetime.utcnow() - breach.get('assigned_at')).total_seconds() / 3600
                    }, default=str)}\n\n"
                
                await asyncio.sleep(1)  # Check every second
                
        except Exception as e:
            logger.error(f"SSE stream error: {e}")
            yield f"event: error\ndata: {json.dumps({'error': str(e)})}\n\n"
    
    headers = {
        "Cache-Control": "no-cache",
        "Connection": "keep-alive",
        "Access-Control-Allow-Origin": "*",
        "Access-Control-Allow-Headers": "*",
    }
    
    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers=headers
    )
