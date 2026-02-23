"""
Real-time WebSocket notification manager
"""
import json
import logging
import asyncio
from typing import Dict, Set, List
from datetime import datetime
from fastapi import WebSocket, WebSocketDisconnect
from app.services.notification_service import get_notification_service

logger = logging.getLogger(__name__)

class WebSocketManager:
    def __init__(self):
        # Store active connections: user_id -> set of WebSockets
        self.active_connections: Dict[str, Set[WebSocket]] = {}
        # Store connection metadata: connection_id -> user_id
        self.connection_users: Dict[str, str] = {}
    
    async def connect(self, websocket: WebSocket, user_id: str):
        """Accept and store WebSocket connection"""
        await websocket.accept()
        
        if user_id not in self.active_connections:
            self.active_connections[user_id] = set()
        
        self.active_connections[user_id].add(websocket)
        
        # Generate a unique connection ID
        connection_id = f"{user_id}_{datetime.now().timestamp()}"
        self.connection_users[connection_id] = user_id
        
        logger.info(f"WebSocket connected for user: {user_id}")
        
        # Send welcome message
        await self.send_to_user(user_id, {
            "type": "connection",
            "message": "Connected to real-time notifications",
            "timestamp": datetime.now().isoformat()
        })
        
        return connection_id
    
    def disconnect(self, websocket: WebSocket, user_id: str):
        """Remove WebSocket connection"""
        if user_id in self.active_connections:
            self.active_connections[user_id].discard(websocket)
            if not self.active_connections[user_id]:
                del self.active_connections[user_id]
        
        # Remove from connection users
        to_remove = [cid for cid, uid in self.connection_users.items() if uid == user_id]
        for cid in to_remove:
            del self.connection_users[cid]
        
        logger.info(f"WebSocket disconnected for user: {user_id}")
    
    async def send_to_user(self, user_id: str, message: dict):
        """Send message to all connections of a user"""
        if user_id in self.active_connections:
            disconnected = set()
            for connection in self.active_connections[user_id]:
                try:
                    await connection.send_text(json.dumps(message))
                except Exception as e:
                    logger.error(f"Failed to send WebSocket message: {e}")
                    disconnected.add(connection)
            
            # Remove dead connections
            for conn in disconnected:
                self.disconnect(conn, user_id)
    
    async def broadcast_to_all(self, message: dict):
        """Broadcast message to all connected users"""
        for user_id in list(self.active_connections.keys()):
            await self.send_to_user(user_id, message)
    
    async def notify_task_assigned(self, file_id: str, employee_name: str, employee_code: str, task_id: str, stage: str):
        """Send real-time notification when task is assigned"""
        message = {
            "type": "task_assigned",
            "title": "✅ Task Assigned",
            "message": f"Task assigned to {employee_name} ({employee_code})",
            "data": {
                "file_id": file_id,
                "task_id": task_id,
                "stage": stage,
                "employee_name": employee_name,
                "employee_code": employee_code
            },
            "timestamp": datetime.now().isoformat(),
            "popup": True  # Indicate that this should show as popup
        }
        
        # Broadcast to all users (in production, you might target specific users/roles)
        await self.broadcast_to_all(message)
        
        # Also store in database using existing notification service
        notification_service = get_notification_service()
        notification_service.send_stage_completion_notification(file_id, stage, employee_code)
        
        logger.info(f"Task assigned notification sent: {file_id} -> {employee_name}")
    
    async def notify_stage_completed(self, file_id: str, employee_name: str, employee_code: str, stage: str, quality_score: float = 0.0):
        """Send real-time notification when a stage is completed"""
        message = {
            "type": "stage_completed",
            "title": f"🎉 {stage.upper()} Completed",
            "message": f"{stage} stage completed by {employee_name}",
            "data": {
                "file_id": file_id,
                "stage": stage,
                "employee_name": employee_name,
                "employee_code": employee_code,
                "quality_score": quality_score
            },
            "timestamp": datetime.now().isoformat(),
            "popup": True
        }
        
        await self.broadcast_to_all(message)
        logger.info(f"Stage completed notification sent: {file_id} - {stage} by {employee_name}")
    
    async def notify_sla_breached(self, file_id: str, stage: str, employee_code: str, employee_name: str = None):
        """Send real-time notification when SLA is breached"""
        message = {
            "type": "sla_breached",
            "title": "⚠️ SLA Breached",
            "message": f"SLA breached for {stage} stage",
            "data": {
                "file_id": file_id,
                "stage": stage,
                "employee_code": employee_code,
                "employee_name": employee_name or f"Employee {employee_code}"
            },
            "timestamp": datetime.now().isoformat(),
            "popup": True
        }
        
        await self.broadcast_to_all(message)
        logger.warning(f"SLA breached notification sent: {file_id} - {stage}")

# Global instance
websocket_manager = WebSocketManager()
