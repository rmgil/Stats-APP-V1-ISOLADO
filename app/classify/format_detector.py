"""
Table format detection for poker hands (9-max vs 6-max)
"""
import re
from typing import Optional, List
from app.parse.hand_splitter import analyze_file_hands

def detect_table_format(content: str) -> str:
    """
    Detect if hands are from 9-max or 6-max table by analyzing individual hands
    Returns: '9-max', '6-max', or 'unknown'
    """
    # Use the hand splitter to analyze all hands in the file
    analysis = analyze_file_hands(content)
    
    # Return the dominant format from the analysis
    dominant = analysis.get('dominant_format', 'unknown')
    
    # If we have a clear dominant format, use it
    if dominant in ['6-max', '9-max']:
        return dominant
    
    # If mixed or unknown, try to make a best guess
    if analysis['6-max_hands'] > 0 and analysis['9-max_hands'] == 0:
        return '6-max'
    elif analysis['9-max_hands'] > 0 and analysis['6-max_hands'] == 0:
        return '9-max'
    
    # Default to 9-max if truly ambiguous
    return '9-max'

def classify_hand_complete(content: str) -> dict:
    """
    Complete classification including tournament type and table format
    """
    from app.classify.run import classify_tournament
    
    # Get tournament classification
    tournament_type = classify_tournament(content)
    
    # Get table format (only relevant for NON-KO)
    table_format = detect_table_format(content) if tournament_type == 'NON-KO' else None
    
    return {
        'tournament_type': tournament_type,
        'table_format': table_format
    }