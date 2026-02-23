from fastapi import APIRouter, HTTPException
from typing import Optional

from app.db.mongodb import get_db
from app.services.file_deduplication_service import FileDeduplicationService

router = APIRouter(prefix="/file-lifecycle", tags=["file-lifecycle"])

@router.get("/{file_id}")
async def get_file_lifecycle(file_id: str):
    """Get complete lifecycle information for a file"""
    try:
        lifecycle = FileDeduplicationService.get_file_lifecycle(file_id)
        
        if not lifecycle:
            raise HTTPException(status_code=404, detail="File not found")
        
        return {
            "success": True,
            "data": lifecycle
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/by-name/{file_name}")
async def find_file_by_name(file_name: str):
    """Find file by filename and return its lifecycle"""
    db = get_db()
    
    # Try exact match first
    file_doc = db.permit_files.find_one({
        'file_info.original_filename': file_name
    })
    
    if not file_doc:
        # Try case-insensitive
        file_doc = db.permit_files.find_one({
            'file_info.original_filename': {'$regex': f'^{file_name}$', '$options': 'i'}
        })
    
    if not file_doc:
        raise HTTPException(status_code=404, detail="File not found with given name")
    
    file_id = file_doc.get('file_id')
    lifecycle = FileDeduplicationService.get_file_lifecycle(file_id)
    
    return {
        "success": True,
        "data": lifecycle
    }

@router.get("/versions/{file_id}")
async def get_file_versions(file_id: str):
    """Get all versions of a file"""
    db = get_db()
    
    file_doc = db.permit_files.find_one(
        {'file_id': file_id}, 
        {'_id': 0, 'file_id': 1, 'file_info.original_filename': 1, 'version_history': 1}
    )
    
    if not file_doc:
        raise HTTPException(status_code=404, detail="File not found")
    
    return {
        "success": True,
        "data": {
            "file_id": file_id,
            "file_name": file_doc.get('file_info', {}).get('original_filename'),
            "versions": file_doc.get('version_history', [])
        }
    }
