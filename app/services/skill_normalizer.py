"""
Skill Normalizer Module

This module provides functionality to normalize employee skill data by:
1. Extracting canonical technical skill keywords from noisy text
2. Removing sentences, verbs, filler words, and repeated content
3. Creating clean skill lists for better semantic matching

The normalization is additive and safe - it doesn't modify existing fields.
"""

import logging
from typing import Dict, List, Set, Any, Optional
from app.core.settings import settings

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

class SkillNormalizer:
    """Normalizes and categorizes employee skills"""
    
    # Technical skill categories
    TECHNICAL_SKILLS_DB = {
        'structural_design': [
            'structural design', 'structural analysis', 'structural calculation',
            'load calculation', 'structural drawing', 'cad design', 'autocad',
            'roof design', 'building design', 'foundation design', 'beam design',
            'truss design', 'rafter design', 'column design'
        ],
        'electrical_design': [
            'electrical design', 'electrical calculation', 'solar design',
            'pv design', 'photovoltaic design', 'inverter design', 'string design',
            'electrical drawing', 'single line diagram', 'three line diagram',
            'conduit sizing', 'wire sizing', 'voltage drop calculation'
        ],
        'quality_control': [
            'quality', 'quality control', 'quality assurance', 'qa', 'qc',
            'review', 'inspection', 'testing', 'audit', 'check', 'verification',
            'validation', 'compliance', 'quality & compliance'
        ],
        'coordination': [
            'coordination', 'project coordination', 'project management',
            'team management', 'team leadership', 'leadership', 'management'
        ]
    }
    
    # Common filler words to ignore
    FILLER_WORDS = {
        'and', 'or', 'the', 'in', 'on', 'at', 'to', 'for', 'of', 'with',
        'by', 'from', 'up', 'about', 'into', 'through', 'during', 'before',
        'after', 'above', 'below', 'between', 'among', 'under', 'over',
        'i', 'me', 'my', 'myself', 'we', 'our', 'ours', 'ourselves',
        'you', 'your', 'yours', 'yourself', 'yourselves', 'he', 'him',
        'his', 'himself', 'she', 'her', 'hers', 'herself', 'it', 'its',
        'itself', 'they', 'them', 'their', 'theirs', 'themselves',
        'what', 'which', 'who', 'whom', 'this', 'that', 'these',
        'those', 'am', 'is', 'are', 'was', 'were', 'be', 'been', 'being',
        'have', 'has', 'had', 'having', 'do', 'does', 'did', 'doing',
        'a', 'an', 'but', 'if', 'because', 'as', 'until', 'while',
        'same', 'other', 'some', 'any', 'no', 'nor', 'not', 'only',
        'own', 'than', 'too', 'very', 'can', 'will', 'just', 'don',
        'should', 'now', 'also', 'well', 'good', 'new', 'old', 'able'
    }
    
    def normalize_employee_skills(self, employee: Dict) -> Dict:
        """Normalize and categorize employee skills"""
        
        # Get raw skills
        raw_skills = employee.get('raw_technical_skills', '') or ''
        raw_strength = employee.get('raw_strength_expertise', '') or ''
        
        # Also check existing technical_skills field
        existing_skills = employee.get('technical_skills', {})
        if isinstance(existing_skills, dict) and existing_skills:
            # Return existing skills if they have the right structure
            return existing_skills
        
        # Combine all text
        all_text = f"{raw_skills} {raw_strength}".lower()
        
        # Extract and categorize skills
        categorized = {
            'structural_design': [],
            'electrical_design': [],
            'coordination': [],
            'quality_control': []  # Add QC category
        }
        
        # Check each skill category
        for category, skills in self.TECHNICAL_SKILLS_DB.items():
            if category == 'coordination_experience':
                key = 'coordination'
            else:
                key = category.replace('_design', '_design').replace('_systems', '_systems').replace('_components', '_components').replace('_compliance', '_compliance').replace('_tools', '_tools')
            
            if key in categorized:
                for skill in skills:
                    # Check if skill keywords are in employee text
                    skill_lower = skill.lower()
                    if any(keyword in all_text for keyword in skill_lower.split()):
                        categorized[key].append(skill.title())
        
        # Additional heuristic extraction for common patterns
        if 'design' in all_text and not categorized['structural_design'] and not categorized['electrical_design']:
            # If design is mentioned but no specific type, categorize based on other keywords
            if any(word in all_text for word in ['structural', 'building', 'load', 'beam', 'column', 'truss']):
                categorized['structural_design'].append('Design')
            elif any(word in all_text for word in ['electrical', 'solar', 'pv', 'wire', 'conduit']):
                categorized['electrical_design'].append('Design')
        
        # Extract coordination keywords
        coordination_keywords = ['coordination', 'coordinate', 'coordinating', 'management', 'team', 'leadership']
        if any(kw in all_text for kw in coordination_keywords) and not categorized['coordination']:
            categorized['coordination'].append('Coordination')
        
        # Remove duplicates
        for key in categorized:
            categorized[key] = list(set(categorized[key]))
        
        return categorized
    
    def extract_keywords(self, text: str) -> List[str]:
        """Extract relevant keywords from text"""
        
        if not text:
            return []
        
        # Clean and split text
        words = re.findall(r'\b[a-z]+\b', text.lower())
        
        # Remove filler words and short words
        keywords = [w for w in words if w not in self.FILLER_WORDS and len(w) > 2]
        
        # Remove duplicates while preserving order
        seen = set()
        unique_keywords = []
        for word in keywords:
            if word not in seen:
                seen.add(word)
                unique_keywords.append(word)
        
        return unique_keywords
    
    def get_skill_summary(self, employee: Dict) -> str:
        """Get formatted skill summary for display"""
        
        skills = employee.get('skills', {})
        summary_parts = []
        
        for category, skill_list in skills.items():
            if skill_list:
                category_name = category.replace('_', ' ').title()
                summary_parts.append(f"{category_name}: {', '.join(skill_list[:3])}")
        
        return ' | '.join(summary_parts)
    
    def get_primary_domain(self, employee: Dict) -> str:
        """Determine employee's primary domain based on skills"""
        
        skills = employee.get('skills', {})
        
        # Count skills per domain
        domain_counts = {
            'Structural Design': len(skills.get('structural_design', [])),
            'Electrical Design': len(skills.get('electrical_design', [])),
            'Coordination': len(skills.get('coordination', []))
        }
        
        # Return domain with most skills
        if domain_counts['Electrical Design'] >= domain_counts['Structural Design']:
            return 'Electrical Design'
        elif domain_counts['Structural Design'] > 0:
            return 'Structural Design'
        else:
            return 'General'
    
    def _is_valid_technical_term(self, term: str) -> bool:
        """Check if a term is a valid technical skill"""
        if not term or len(term) < 3:
            return False
        
        term_lower = term.lower()
        
        # Exclude filler words and common non-technical terms
        if term_lower in self.FILLER_WORDS:
            return False
        
        # Include if it's in our technical skills database
        if term_lower in self.all_technical_skills:
            return True
        
        # Include if it contains technical keywords
        technical_keywords = {
            'solar', 'pv', 'photovoltaic', 'electrical', 'structural', 'roof',
            'design', 'analysis', 'calculation', 'drawing', 'cad', 'autocad',
            'inverter', 'module', 'panel', 'battery', 'wire', 'conduit',
            'beam', 'truss', 'rafter', 'column', 'code', 'permit'
        }
        
        return any(keyword in term_lower for keyword in technical_keywords)
    
    def _clean_skill_term(self, term: str) -> str:
        """Clean a skill term by removing punctuation and normalizing"""
        if not term:
            return ""
        
        # Remove common punctuation
        term = re.sub(r'[^\w\s-]', ' ', term)
        
        # Normalize whitespace
        term = re.sub(r'\s+', ' ', term).strip()
        
        # Remove leading/trailing common words
        words = term.split()
        if len(words) > 1:
            # Remove leading filler words
            while words and words[0].lower() in self.FILLER_WORDS:
                words.pop(0)
            # Remove trailing filler words
            while words and words[-1].lower() in self.FILLER_WORDS:
                words.pop()
        
        return ' '.join(words).title()  # Title case for consistency


# Export the main class
__all__ = ['SkillNormalizer']
