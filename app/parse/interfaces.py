"""
Parser interface definitions using Python Protocol.
Defines contracts that site-specific parsers must implement.
"""

from typing import Protocol, List
from .schemas import Hand


class SiteParser(Protocol):
    """
    Protocol that all site-specific parsers must implement.
    Each parser handles a specific poker site format.
    """
    
    def detect(self, text: str) -> bool:
        """
        Detect if this parser can handle the given text.
        
        Args:
            text: Raw hand history text
            
        Returns:
            True if this parser recognizes the format
        """
        ...
    
    def parse_tournament(
        self, 
        text: str, 
        file_id: str, 
        hero_aliases: dict
    ) -> List[Hand]:
        """
        Parse all hands from a tournament file.
        
        Args:
            text: Complete tournament text with multiple hands
            file_id: Unique identifier for this file
            hero_aliases: Dict mapping sites to hero name aliases
            
        Returns:
            List of parsed Hand objects
        """
        ...