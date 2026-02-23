"""
WebSocket router for real-time notifications
"""
import logging
import json
from typing import Optional
from fastapi import APIRouter, WebSocket, WebSocketDisconnect, Query
from app.services.websocket_manager import websocket_manager

logger = logging.getLogger(__name__)
router = APIRouter()

@router.websocket("/ws/notifications")
async def websocket_notifications(
    websocket: WebSocket,
    token: str = Query(...),
    user_id: Optional[str] = None
):
    """WebSocket endpoint for real-time notifications"""
    try:
        # Authenticate user (optional - you might want to implement this)
        # For now, we'll use the user_id from query param or token
        if not user_id:
            # In production, decode token to get user_id
            user_id = "user_123"  # Placeholder
        
        # Connect to WebSocket manager
        connection_id = await websocket_manager.connect(websocket, user_id)
        
        try:
            # Keep connection alive and listen for messages
            while True:
                # Receive message from client (optional, for ping/pong or commands)
                data = await websocket.receive_text()
                
                # Parse client message
                try:
                    message = json.loads(data)
                    
                    # Handle different message types
                    if message.get("type") == "ping":
                        await websocket.send_text(json.dumps({
                            "type": "pong",
                            "timestamp": message.get("timestamp")
                        }))
                    elif message.get("type") == "mark_read":
                        # Handle marking notifications as read
                        notification_id = message.get("notification_id")
                        if notification_id:
                            # TODO: Mark notification as read in database
                            pass
                    
                except json.JSONDecodeError:
                    logger.warning(f"Invalid JSON received: {data}")
                    await websocket.send_text(json.dumps({
                        "type": "error",
                        "message": "Invalid JSON format"
                    }))
                
        except WebSocketDisconnect:
            logger.info(f"WebSocket disconnected for user: {user_id}")
        except Exception as e:
            logger.error(f"WebSocket error for user {user_id}: {e}")
        finally:
            websocket_manager.disconnect(websocket, user_id)
            
    except Exception as e:
        logger.error(f"Failed to establish WebSocket connection: {e}")
        await websocket.close(code=1000, reason="Connection failed")

@router.websocket("/ws/employee/{employee_code}")
async def websocket_employee_notifications(
    websocket: WebSocket,
    employee_code: str,
    token: str = Query(...)
):
    """WebSocket endpoint for employee-specific notifications"""
    try:
        # Connect to WebSocket manager
        connection_id = await websocket_manager.connect(websocket, employee_code)
        
        try:
            # Keep connection alive
            while True:
                data = await websocket.receive_text()
                
                # Handle ping/pong
                try:
                    message = json.loads(data)
                    if message.get("type") == "ping":
                        await websocket.send_text(json.dumps({
                            "type": "pong",
                            "timestamp": message.get("timestamp")
                        }))
                except json.JSONDecodeError:
                    pass
                
        except WebSocketDisconnect:
            logger.info(f"Employee WebSocket disconnected: {employee_code}")
        except Exception as e:
            logger.error(f"Employee WebSocket error {employee_code}: {e}")
        finally:
            websocket_manager.disconnect(websocket, employee_code)
            
    except Exception as e:
        logger.error(f"Failed to establish employee WebSocket connection: {e}")
        await websocket.close(code=1000, reason="Connection failed")
