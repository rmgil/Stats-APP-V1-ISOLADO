#!/usr/bin/env python
"""Create synthetic test hands for postflop validation"""
import json
import os
from datetime import datetime

def create_test_hands():
    """Create synthetic hands with various postflop scenarios"""
    hands = []
    
    # Hand 1: Hero CBet Flop IP
    hands.append({
        "hand_id": "TEST001",
        "site": "pokerstars",
        "file_id": "test_file",
        "timestamp": datetime.now().isoformat(),
        "hero": "Hero",
        "table_max": 6,
        "button_seat": 3,
        "players": [
            {"name": "Hero", "seat": 1, "stack": 100.0, "cards": ["As", "Ks"]},
            {"name": "Villain", "seat": 3, "stack": 100.0}
        ],
        "streets": {
            "preflop": {
                "actions": [
                    {"actor": "Hero", "type": "RAISE", "amount": 3.0},
                    {"actor": "Villain", "type": "CALL", "amount": 3.0}
                ],
                "pot": 6.5
            },
            "flop": {
                "cards": ["Kd", "7h", "2c"],
                "actions": [
                    {"actor": "Villain", "type": "CHECK"},
                    {"actor": "Hero", "type": "BET", "amount": 4.0},
                    {"actor": "Villain", "type": "FOLD"}
                ],
                "pot": 6.5
            }
        }
    })
    
    # Hand 2: Hero faces CBet and folds IP
    hands.append({
        "hand_id": "TEST002",
        "site": "pokerstars",
        "file_id": "test_file",
        "timestamp": datetime.now().isoformat(),
        "hero": "Hero",
        "table_max": 6,
        "button_seat": 1,
        "players": [
            {"name": "Hero", "seat": 1, "stack": 100.0, "cards": ["Jh", "Th"]},
            {"name": "Villain", "seat": 3, "stack": 100.0}
        ],
        "streets": {
            "preflop": {
                "actions": [
                    {"actor": "Villain", "type": "RAISE", "amount": 3.0},
                    {"actor": "Hero", "type": "CALL", "amount": 3.0}
                ],
                "pot": 6.5
            },
            "flop": {
                "cards": ["Ad", "Kc", "5s"],
                "actions": [
                    {"actor": "Villain", "type": "BET", "amount": 4.0},
                    {"actor": "Hero", "type": "FOLD"}
                ],
                "pot": 6.5
            }
        }
    })
    
    # Hand 3: Hero donk bets flop OOP
    hands.append({
        "hand_id": "TEST003",
        "site": "pokerstars",
        "file_id": "test_file",
        "timestamp": datetime.now().isoformat(),
        "hero": "Hero",
        "table_max": 6,
        "button_seat": 3,
        "players": [
            {"name": "Hero", "seat": 1, "stack": 100.0, "cards": ["Kh", "Qh"]},
            {"name": "Villain", "seat": 3, "stack": 100.0}
        ],
        "streets": {
            "preflop": {
                "actions": [
                    {"actor": "Villain", "type": "RAISE", "amount": 3.0},
                    {"actor": "Hero", "type": "CALL", "amount": 3.0}
                ],
                "pot": 6.5
            },
            "flop": {
                "cards": ["Kd", "9h", "4c"],
                "actions": [
                    {"actor": "Hero", "type": "BET", "amount": 4.0},
                    {"actor": "Villain", "type": "CALL", "amount": 4.0}
                ],
                "pot": 14.5
            }
        }
    })
    
    # Hand 4: Hero raises vs CBet IP
    hands.append({
        "hand_id": "TEST004", 
        "site": "pokerstars",
        "file_id": "test_file",
        "timestamp": datetime.now().isoformat(),
        "hero": "Hero",
        "table_max": 6,
        "button_seat": 1,
        "players": [
            {"name": "Hero", "seat": 1, "stack": 100.0, "cards": ["Ac", "Qc"]},
            {"name": "Villain", "seat": 3, "stack": 100.0}
        ],
        "streets": {
            "preflop": {
                "actions": [
                    {"actor": "Villain", "type": "RAISE", "amount": 3.0},
                    {"actor": "Hero", "type": "CALL", "amount": 3.0}
                ],
                "pot": 6.5
            },
            "flop": {
                "cards": ["Qd", "Jc", "3s"],
                "actions": [
                    {"actor": "Villain", "type": "BET", "amount": 4.0},
                    {"actor": "Hero", "type": "RAISE", "amount": 12.0},
                    {"actor": "Villain", "type": "FOLD"}
                ],
                "pot": 10.5
            }
        }
    })
    
    # Hand 5: Goes to showdown (WTSD)
    hands.append({
        "hand_id": "TEST005",
        "site": "pokerstars",
        "file_id": "test_file",
        "timestamp": datetime.now().isoformat(),
        "hero": "Hero",
        "table_max": 6,
        "button_seat": 1,
        "players": [
            {"name": "Hero", "seat": 1, "stack": 100.0, "cards": ["As", "Js"]},
            {"name": "Villain", "seat": 3, "stack": 100.0}
        ],
        "streets": {
            "preflop": {
                "actions": [
                    {"actor": "Hero", "type": "RAISE", "amount": 3.0},
                    {"actor": "Villain", "type": "CALL", "amount": 3.0}
                ],
                "pot": 6.5
            },
            "flop": {
                "cards": ["Jd", "8h", "4c"],
                "actions": [
                    {"actor": "Villain", "type": "CHECK"},
                    {"actor": "Hero", "type": "BET", "amount": 4.0},
                    {"actor": "Villain", "type": "CALL", "amount": 4.0}
                ],
                "pot": 14.5
            },
            "turn": {
                "cards": ["2s"],
                "actions": [
                    {"actor": "Villain", "type": "CHECK"},
                    {"actor": "Hero", "type": "CHECK"}
                ],
                "pot": 14.5
            },
            "river": {
                "cards": ["6h"],
                "actions": [
                    {"actor": "Villain", "type": "CHECK"},
                    {"actor": "Hero", "type": "BET", "amount": 8.0},
                    {"actor": "Villain", "type": "CALL", "amount": 8.0}
                ],
                "pot": 30.5
            }
        }
    })
    
    # Hand 6: Hero bets vs missed CBet
    hands.append({
        "hand_id": "TEST006",
        "site": "pokerstars",
        "file_id": "test_file", 
        "timestamp": datetime.now().isoformat(),
        "hero": "Hero",
        "table_max": 6,
        "button_seat": 3,
        "players": [
            {"name": "Hero", "seat": 1, "stack": 100.0, "cards": ["9s", "9h"]},
            {"name": "Villain", "seat": 3, "stack": 100.0}
        ],
        "streets": {
            "preflop": {
                "actions": [
                    {"actor": "Villain", "type": "RAISE", "amount": 3.0},
                    {"actor": "Hero", "type": "CALL", "amount": 3.0}
                ],
                "pot": 6.5
            },
            "flop": {
                "cards": ["7d", "5c", "2h"],
                "actions": [
                    {"actor": "Villain", "type": "CHECK"},
                    {"actor": "Hero", "type": "CHECK"}
                ],
                "pot": 6.5
            },
            "turn": {
                "cards": ["Kc"],
                "actions": [
                    {"actor": "Villain", "type": "CHECK"},
                    {"actor": "Hero", "type": "BET", "amount": 4.0},
                    {"actor": "Villain", "type": "FOLD"}
                ],
                "pot": 6.5
            }
        }
    })
    
    return hands

def main():
    """Create test data files"""
    # Create parsed directory if needed
    os.makedirs("parsed", exist_ok=True)
    
    # Generate test hands
    hands = create_test_hands()
    
    # Save to JSONL
    output_file = "parsed/hands.jsonl"
    with open(output_file, "w", encoding="utf-8") as f:
        for hand in hands:
            f.write(json.dumps(hand) + "\n")
    
    print(f"Created {len(hands)} test hands in {output_file}")
    return output_file

if __name__ == "__main__":
    main()