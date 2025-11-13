"""
Create partitions from enriched hands
"""
import os
import json
from pathlib import Path
from typing import Dict, Any

def create_partitions(input_file: str, output_dir: str) -> Dict[str, Any]:
    """
    Create partitioned JSONL files by month and category
    """
    os.makedirs(output_dir, exist_ok=True)
    
    partitions = {}
    hands_by_partition = {}
    
    # Read enriched hands
    with open(input_file, 'r') as f:
        for line in f:
            hand = json.loads(line)
            
            # Create partition key
            month = hand.get("month", "unknown")
            category = hand.get("category", "unknown")
            partition_key = f"{month}_{category.lower()}"
            
            if partition_key not in hands_by_partition:
                hands_by_partition[partition_key] = []
            
            hands_by_partition[partition_key].append(hand)
    
    # Write partition files
    for partition_key, hands in hands_by_partition.items():
        output_file = os.path.join(output_dir, f"{partition_key}.jsonl")
        
        with open(output_file, 'w') as f:
            for hand in hands:
                f.write(json.dumps(hand) + '\n')
        
        partitions[partition_key] = {
            "file": f"{partition_key}.jsonl",
            "hands": len(hands)
        }
    
    # Save partition counts
    counts_file = os.path.join(output_dir, "partition_counts.json")
    with open(counts_file, 'w') as f:
        json.dump(partitions, f, indent=2)
    
    return {
        "partitions_created": len(partitions),
        "total_hands": sum(p["hands"] for p in partitions.values())
    }