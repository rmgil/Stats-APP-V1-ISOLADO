"""
Tournament text classification API wrapper.
Reutilizes existing regex patterns and logic from main.py
"""

import re
from pathlib import Path
from typing import TypedDict, Literal
import sys
import os
import chardet

# Add parent directory to path to import from main
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

# Import existing regex patterns from main.py
from main import WORD_MYSTERY, WORD_PKO

# Type definitions
ClassName = Literal["mystery", "PKO", "non-KO", "unknown"]

class DetectResult(TypedDict):
    class_name: ClassName
    reason: str        # string curta com a palavra-chave que bateu
    score: float       # 1.0 se match direto; 0.5 se heurística
    is_mystery: bool
    is_pko: bool
    is_nonko: bool
    encoding: str
    bytes: int


def classify_tournament_text(text: str, filename: str = "") -> DetectResult:
    """
    Classifica texto de torneio usando a mesma lógica do process_txt_tree.
    
    Args:
        text: Conteúdo do texto a classificar
        filename: Nome do ficheiro (opcional) para verificação adicional
    
    Returns:
        DetectResult com classificação e detalhes
    """
    # Initialize result
    result: DetectResult = {
        "class_name": "unknown",
        "reason": "",
        "score": 0.0,
        "is_mystery": False,
        "is_pko": False,
        "is_nonko": False,
        "encoding": "utf-8",
        "bytes": len(text.encode('utf-8')) if text else 0
    }
    
    # Check if text is valid
    if not text:
        result["reason"] = "empty content"
        result["score"] = 0.0
        return result
    
    # Check for too many replacement characters (likely binary)
    if text.count('�') > len(text) * 0.3:
        result["reason"] = "invalid or binary content"
        result["score"] = 0.5
        return result
    
    # Check for mystery words in filename and content (same logic as main.py)
    mystery_in_filename = WORD_MYSTERY.search(filename) if filename else None
    mystery_in_content = WORD_MYSTERY.search(text) if text else None
    
    if mystery_in_filename or mystery_in_content:
        result["is_mystery"] = True
        result["class_name"] = "mystery"
        result["score"] = 1.0
        
        if mystery_in_filename:
            match = mystery_in_filename.group(1) if mystery_in_filename else "mystery"
            result["reason"] = f"'{match}' in filename"
        elif mystery_in_content:
            match = mystery_in_content.group(1) if mystery_in_content else "mystery"
            result["reason"] = f"'{match}' in content"
        
        return result
    
    # Check for PKO-related words in filename and content
    pko_in_filename = WORD_PKO.search(filename) if filename else None
    pko_in_content = WORD_PKO.search(text) if text else None
    
    if pko_in_filename or pko_in_content:
        result["is_pko"] = True
        result["class_name"] = "PKO"
        result["score"] = 1.0
        
        if pko_in_filename:
            match = pko_in_filename.group(1) if pko_in_filename else "pko"
            result["reason"] = f"'{match}' in filename"
        elif pko_in_content:
            match = pko_in_content.group(1) if pko_in_content else "pko"
            result["reason"] = f"'{match}' in content"
        
        return result
    
    # If neither mystery nor PKO, it's non-KO
    result["is_nonko"] = True
    result["class_name"] = "non-KO"
    result["score"] = 1.0
    result["reason"] = "no special keywords found"
    
    return result


def classify_file(path: str) -> DetectResult:
    """
    Classifica um ficheiro baseado no seu conteúdo e nome.
    Usa chardet para deteção inteligente de encoding.
    
    Args:
        path: Caminho para o ficheiro a classificar
    
    Returns:
        DetectResult com classificação e detalhes
    """
    file_path = Path(path)
    
    # Check if file exists
    if not file_path.exists():
        return DetectResult(
            class_name="unknown",
            reason="file not found",
            score=0.0,
            is_mystery=False,
            is_pko=False,
            is_nonko=False,
            encoding="unknown",
            bytes=0
        )
    
    # Get file size
    file_size = file_path.stat().st_size
    
    # Check if it's a supported file type
    if file_path.suffix.lower() not in ['.txt', '.xml']:
        return DetectResult(
            class_name="unknown",
            reason=f"unsupported file type: {file_path.suffix}",
            score=0.0,
            is_mystery=False,
            is_pko=False,
            is_nonko=False,
            encoding="unknown",
            bytes=file_size
        )
    
    # Detect encoding using chardet
    with open(file_path, 'rb') as f:
        raw_data = f.read()
        detection = chardet.detect(raw_data)
    
    detected_encoding = detection.get('encoding', 'utf-8') if detection else 'utf-8'
    confidence = detection.get('confidence', 0) if detection else 0
    
    # If confidence is low, try common encodings
    if confidence < 0.7:
        encodings_to_try = ['utf-8', 'latin1', 'cp1252', 'iso-8859-1']
        if detected_encoding and detected_encoding not in encodings_to_try:
            encodings_to_try.insert(0, detected_encoding)
    else:
        encodings_to_try = [detected_encoding, 'utf-8', 'latin1']
    
    # Try to decode with detected or fallback encodings
    content = None
    used_encoding = "unknown"
    
    for encoding in encodings_to_try:
        try:
            if encoding:
                content = raw_data.decode(encoding, errors='replace')
                used_encoding = encoding
                break
        except (UnicodeDecodeError, LookupError):
            continue
    
    if content is None:
        # Last resort: use utf-8 with replace errors
        content = raw_data.decode('utf-8', errors='replace')
        used_encoding = "utf-8-fallback"
    
    # Get filename for classification
    filename = file_path.name
    
    # Classify using the same logic
    result = classify_tournament_text(content, filename)
    
    # Update encoding and bytes
    result["encoding"] = used_encoding
    result["bytes"] = file_size
    
    return result


# Helper function to check classification consistency
def get_class_name(filename: str, content: str) -> ClassName:
    """
    Helper function that returns just the class name.
    Mantém compatibilidade com a lógica existente.
    """
    result = classify_tournament_text(content, filename)
    return result["class_name"]