"""
Parse and derive hands from classified files
"""
import os
import json
from pathlib import Path
from typing import Dict, Any

def derive_hands_enriched(classified_dir: str, output_file: str) -> Dict[str, Any]:
    """
    Parse classified hands and create enriched JSONL output
    """
    hands_count = 0
    
    # Process all classified files
    classified_path = Path(classified_dir)
    
    with open(output_file, 'w') as out_f:
        for category_dir in classified_path.iterdir():
            if not category_dir.is_dir():
                continue
            
            category = category_dir.name
            
            for txt_file in category_dir.glob("*.txt"):
                # Simple hand parsing (placeholder)
                content = txt_file.read_text(errors='ignore')
                
                # Split by common hand delimiters
                hands = content.split("\n\n\n")
                
                for i, hand_text in enumerate(hands):
                    if len(hand_text.strip()) < 50:  # Skip too short
                        continue
                    
                    # Create minimal hand record
                    hand_record = {
                        "hand_id": f"{txt_file.stem}_{i}",
                        "category": category,
                        "file": txt_file.name,
                        "text_length": len(hand_text),
                        "has_showdown": "shows" in hand_text.lower(),
                        "month": "2024-11"  # Placeholder
                    }
                    
                    out_f.write(json.dumps(hand_record) + '\n')
                    hands_count += 1
    
    return {
        "hands_parsed": hands_count,
        "output_file": output_file
    }