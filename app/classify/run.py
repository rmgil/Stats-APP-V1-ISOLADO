"""
Classification runner for pipeline
"""
import os
import re
import json
import shutil
from pathlib import Path
from typing import Dict, Any, Optional, Tuple

# Regex patterns for tournament classification (case-insensitive)
PKO_PATTERNS = [
    r'\bbounty\b',
    r'\bprogressive\b',
    r'\bknockout\b',
    r'\bko\b(?!\w)',  # KO as whole word
    r'\bpko\b',
    r'\bbounty\s+hunters?\b'
]

MYSTERY_PATTERNS = [
    r'\bmystery\b.*\bbounty\b',
    r'\bbounty\b.*\bmystery\b',
    r'\bmysteries\b'
]

def is_pokerstars_hand(content: str) -> bool:
    """Check if this is a PokerStars hand history."""
    # Check for PokerStars marker in the first 500 chars (to handle prepended filename)
    return "PokerStars Hand #" in content[:500] if len(content) > 0 else False

def detect_pokerstars_buyin_format(content: str) -> Tuple[bool, bool]:
    """
    Detect PokerStars buy-in format to determine tournament type.
    Returns (is_three_part, has_bounty_in_seats)
    """
    # Find the actual PokerStars line (skip prepended filename if present)
    lines = content.split('\n') if content else []
    first_line = ''
    for line in lines[:5]:  # Check first 5 lines
        if 'PokerStars' in line:
            first_line = line
            break
    if not first_line and lines:
        first_line = lines[0]
    
    # Check if it's a 3-part buy-in (PKO or Mystery format)
    three_part_match = re.search(r'\$[\d.]+\+\$[\d.]+\+\$[\d.]+', first_line)
    is_three_part = three_part_match is not None
    
    # Check if bounty appears in player seats (only in first 1000 chars for efficiency)
    # Look for pattern like: "(5000 in chips, $7.50 bounty)"
    has_bounty_in_seats = bool(re.search(r'\(\d+\s+in\s+chips,\s+\$[\d.]+\s+bounty\)', content[:2000]))
    
    return is_three_part, has_bounty_in_seats

def classify_tournament(content: str) -> str:
    """
    Classify tournament based on content using robust regex matching
    
    Returns: 'MYSTERIES', 'PKO', or 'NON-KO'
    """
    content_lower = content.lower()
    
    # Special handling for PokerStars
    if is_pokerstars_hand(content):
        is_three_part, has_bounty_in_seats = detect_pokerstars_buyin_format(content)
        
        if is_three_part:
            # 3-part buy-in: either PKO or Mystery
            if has_bounty_in_seats:
                return "PKO"  # Has bounty in seats = regular PKO
            else:
                return "MYSTERIES"  # No bounty in seats = Mystery
        else:
            # 2-part buy-in = regular tournament
            # Still check for Mystery/PKO in text (some might have it in tournament name)
            for pattern in MYSTERY_PATTERNS:
                if re.search(pattern, content_lower, re.IGNORECASE):
                    return "MYSTERIES"
            
            for pattern in PKO_PATTERNS:
                if re.search(pattern, content_lower, re.IGNORECASE):
                    return "PKO"
                    
            return "NON-KO"
    
    # For non-PokerStars sites (GG, 888, etc), use original logic
    # Check for Mystery Bounty first (highest priority)
    for pattern in MYSTERY_PATTERNS:
        if re.search(pattern, content_lower, re.IGNORECASE):
            return "MYSTERIES"
    
    # Check for PKO/Bounty
    for pattern in PKO_PATTERNS:
        if re.search(pattern, content_lower, re.IGNORECASE):
            return "PKO"
    
    # Default to NON-KO
    return "NON-KO"

def extract_tournament_info(content: str) -> Optional[str]:
    """
    Extract tournament name from hand history
    Works with PokerStars, GGPoker, 888
    """
    # PokerStars format: Tournament #999999999, $10+$10 USD Hold'em Progressive Knockout - Level
    ps_match = re.search(r'Tournament #\d+,\s*([^-\n]+?)(?:\s*-\s*Level|\n)', content[:500])
    if ps_match:
        # Extract the part after the buy-in
        tournament_part = ps_match.group(1)
        # Remove the buy-in part (e.g., "$10+$10 USD")
        clean_match = re.search(r'(?:\$[\d.]+\+\$[\d.]+\s*\w+\s+)?(.+)', tournament_part)
        if clean_match:
            return clean_match.group(1).strip()
    
    # GGPoker format: Tournament #999999 "Name Here"
    gg_match = re.search(r'Tournament\s+#\d+\s*"([^"]+)"', content[:500])
    if gg_match:
        return gg_match.group(1).strip()
    
    # 888 format: Tournament ID: 999999 - Name
    e88_match = re.search(r'Tournament\s+ID:\s*\d+\s*-\s*([^\n]+)', content[:500])
    if e88_match:
        return e88_match.group(1).strip()
    
    return None

def classify_hands(input_dir: str, output_dir: str, manifest_path: str) -> Dict[str, Any]:
    """
    Classify hand history files into categories with robust detection
    """
    os.makedirs(output_dir, exist_ok=True)
    
    # Create category directories
    categories = ["NON-KO", "PKO", "MYSTERIES"]
    for category in categories:
        os.makedirs(os.path.join(output_dir, category), exist_ok=True)
    
    # Process input files
    classified_files = []
    input_path = Path(input_dir)
    
    for txt_file in input_path.glob("*.txt"):
        content = txt_file.read_text(errors='ignore')
        
        # Extract tournament info for better classification
        tournament_name = extract_tournament_info(content)
        
        # Classify based on filename + tournament name + hand history content
        # This ensures PKO/Mystery keywords in filename are detected for 888poker
        classification_text = txt_file.name + " " + content[:5000]  # Include filename + first 5KB for performance
        if tournament_name:
            classification_text = txt_file.name + " " + tournament_name + " " + content[:5000]
        
        category = classify_tournament(classification_text)
        
        # Copy file to classified directory
        dest_path = os.path.join(output_dir, category, txt_file.name)
        shutil.copy2(txt_file, dest_path)
        
        classified_files.append({
            "filename": txt_file.name,
            "category": category,
            "tournament_name": tournament_name,
            "size": txt_file.stat().st_size
        })
    
    # Create manifest
    manifest = {
        "total_files": len(classified_files),
        "categories": {cat: sum(1 for f in classified_files if f["category"] == cat) for cat in categories},
        "files": classified_files
    }
    
    with open(manifest_path, 'w') as f:
        json.dump(manifest, f, indent=2)
    
    return manifest