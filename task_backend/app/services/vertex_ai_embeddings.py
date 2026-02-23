"""
Vertex AI Gemini Embedding Service
Generates embeddings for employee skills and task descriptions
"""
import os
from typing import List, Dict, Any, Optional
import numpy as np
from google.cloud import aiplatform
from vertexai.language_models import TextEmbeddingModel
import vertexai
import logging
from app.core.settings import settings

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

class VertexAIEmbeddingService:
    """Service for generating embeddings using Vertex AI Gemini
    
    IMPORTANT: 
    - text-embedding-004 is a pure embedding model (NO thinking/reasoning)
    - Directly converts text → 768-dimensional vector
    - Deterministic output (same input = same output)
    - No temperature, no thinking, no reasoning overhead
    - Only user can modify behavior from code
    """
    
    def __init__(self):
        self.initialized = False
        self.model = None
        self.embedding_dimension = 768  # text-embedding-004 dimension
        # Note: No thinking/reasoning parameters - this is a direct embedding model
        
    def initialize(self):
        """Initialize Vertex AI with credentials"""
        if self.initialized:
            return
            
        try:
            # Initialize Vertex AI
            if settings.use_vertex_ai:
                project_id = settings.vertex_ai_project_id or settings.project_id
                location = settings.vertex_ai_region or settings.location
                
                if not project_id:
                    raise ValueError("Vertex AI project ID not configured")
                
                # Set credentials if provided
                if settings.google_application_credentials:
                    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = settings.google_application_credentials
                
                # Initialize Vertex AI
                vertexai.init(project=project_id, location=location)
                
                # Load embedding model
                model_name = settings.vertex_ai_embedding_model_name
                self.model = TextEmbeddingModel.from_pretrained(model_name)
                
                self.initialized = True
                print(f"✅ Vertex AI initialized with model: {model_name}")
            else:
                print("⚠️ Vertex AI not enabled, using mock embeddings")
        except Exception as e:
            print(f"❌ Failed to initialize Vertex AI: {e}")
            print("⚠️ Falling back to mock embeddings")
            self.initialized = False
    
    def generate_embedding(self, text: str) -> List[float]:
        """Generate embedding for a single text
        
        IMPORTANT: 
        - NO thinking/reasoning involved
        - Direct text → vector conversion
        - Deterministic output (same input = same output)
        - No LLM reasoning overhead
        - Pure mathematical transformation
        
        Returns:
            List[float]: 768-dimensional embedding vector
        """
        if not self.initialized or not self.model:
            return self._mock_embedding(text)
        
        try:
            # Direct embedding generation (NO thinking, NO reasoning)
            embeddings = self.model.get_embeddings([text])
            if embeddings and len(embeddings) > 0:
                return embeddings[0].values
            else:
                return self._mock_embedding(text)
        except Exception as e:
            print(f"❌ Error generating embedding: {e}")
            return self._mock_embedding(text)
    
    def generate_embeddings_batch(self, texts: List[str]) -> List[List[float]]:
        """Generate embeddings for multiple texts"""
        if not self.initialized or not self.model:
            return [self._mock_embedding(text) for text in texts]
        
        try:
            # Vertex AI supports batch processing
            embeddings = self.model.get_embeddings(texts)
            return [emb.values for emb in embeddings]
        except Exception as e:
            print(f"❌ Error generating batch embeddings: {e}")
            return [self._mock_embedding(text) for text in texts]
    
    def _mock_embedding(self, text: str) -> List[float]:
        """Fallback mock embedding for development/testing"""
        import hashlib
        hash_obj = hashlib.md5(text.encode())
        hex_dig = hash_obj.hexdigest()
        
        embedding = []
        for i in range(0, len(hex_dig), 2):
            hex_pair = hex_dig[i:i+2]
            val = int(hex_pair, 16) / 255.0 * 2 - 1
            embedding.append(val)
        
        # Extend to 768 dimensions (Gemini embedding size)
        while len(embedding) < 768:
            embedding.extend(embedding[:min(768 - len(embedding), len(embedding))])
        
        return embedding[:768]
    
    def cosine_similarity(self, embedding1: List[float], embedding2: List[float]) -> float:
        """Calculate cosine similarity between two embeddings"""
        a = np.array(embedding1)
        b = np.array(embedding2)
        return float(np.dot(a, b) / (np.linalg.norm(a) * np.linalg.norm(b)))
    
    def prepare_employee_text(self, employee: Dict[str, Any]) -> str:
        """Prepare employee data for embedding generation"""
        # Extract essential fields
        name = employee.get('employee_name', '')
        role = employee.get('current_role', '')
        skills = employee.get('technical_skills', [])
        prev_exp = employee.get('previous_experience_years', 0)
        curr_exp = employee.get('current_experience_years', 0)
        shift = employee.get('shift', '')
        status = employee.get('employee_status', {}).get('availability', '')
        
        # Create comprehensive text representation
        skills_text = ', '.join(skills) if isinstance(skills, list) else str(skills)
        
        text = f"""
        Employee Profile:
        Name: {name}
        Role: {role}
        Technical Skills: {skills_text}
        Experience: {prev_exp} years previous experience, {curr_exp} years current experience
        Total Experience: {prev_exp + curr_exp} years
        Shift: {shift}
        Availability: {status}
        """
        
        return text.strip()
    
    def prepare_task_text(self, task_description: str, additional_context: Optional[Dict[str, Any]] = None) -> str:
        """Prepare task description for embedding generation"""
        text = f"Task Description: {task_description}"
        
        if additional_context:
            if additional_context.get('file_id'):
                text += f"\nRelated File: {additional_context['file_id']}"
            if additional_context.get('priority'):
                text += f"\nPriority: {additional_context['priority']}"
            if additional_context.get('deadline'):
                text += f"\nDeadline: {additional_context['deadline']}"
            if additional_context.get('required_skills'):
                skills = ', '.join(additional_context['required_skills'])
                text += f"\nRequired Skills: {skills}"
        
        return text.strip()

# Global instance
embedding_service = VertexAIEmbeddingService()

def get_embedding_service() -> VertexAIEmbeddingService:
    """Get the global embedding service instance"""
    if not embedding_service.initialized:
        embedding_service.initialize()
    return embedding_service
