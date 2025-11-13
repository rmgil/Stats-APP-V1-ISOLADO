"""
Streaming hand history processor to handle large files efficiently
"""
import re
from typing import Iterator, Optional
import logging

logger = logging.getLogger(__name__)

class HandStreamer:
    """Stream hands from a file without loading entire file into memory"""
    
    SITE_PATTERNS = {
        'pokerstars': r'^PokerStars Hand #',
        'gg': r'^Poker Hand #',
        'winamax': r'^Winamax Poker',
        '888': r'^#Game No :',
        'wpn': r'^(ACR Poker Hand|PokerKing Hand|Game Hand #)'
    }
    
    def __init__(self, file_path: str, site: Optional[str] = None):
        """
        Initialize hand streamer
        
        Args:
            file_path: Path to hand history file
            site: Optional site name (auto-detected if not provided)
        """
        self.file_path = file_path
        self.site = site
        self._detect_pattern()
    
    def _detect_pattern(self):
        """Detect the hand boundary pattern based on site"""
        if self.site and self.site in self.SITE_PATTERNS:
            self.boundary_pattern = re.compile(self.SITE_PATTERNS[self.site], re.MULTILINE)
        else:
            with open(self.file_path, 'r', encoding='utf-8', errors='ignore') as f:
                sample = f.read(10000)
                for site, pattern in self.SITE_PATTERNS.items():
                    if re.search(pattern, sample, re.MULTILINE):
                        self.site = site
                        self.boundary_pattern = re.compile(pattern, re.MULTILINE)
                        logger.info(f"Auto-detected {site} format in {self.file_path}")
                        return
                
                self.boundary_pattern = None
                logger.warning(f"Could not detect hand pattern in {self.file_path}")
    
    def stream_hands(self, chunk_size: int = 8192) -> Iterator[str]:
        """
        Stream hands one at a time from file
        
        Args:
            chunk_size: Size of chunks to read (bytes)
            
        Yields:
            Individual hand history text strings
        """
        if not self.boundary_pattern:
            logger.error(f"No boundary pattern for {self.file_path}")
            return
        
        with open(self.file_path, 'r', encoding='utf-8', errors='ignore') as f:
            current_hand = []
            buffer = ''
            
            while True:
                chunk = f.read(chunk_size)
                if not chunk:
                    # EOF: append residual buffer and yield final hand
                    if buffer:
                        current_hand.append(buffer)
                    if current_hand:
                        hand_text = '\n'.join(current_hand).strip()
                        if hand_text:
                            yield hand_text
                    break
                
                buffer += chunk
                
                lines = buffer.split('\n')
                buffer = lines[-1]  # Keep incomplete line in buffer
                
                for line in lines[:-1]:
                    if self.boundary_pattern.match(line):
                        # Start of new hand - yield previous hand if exists
                        if current_hand:
                            hand_text = '\n'.join(current_hand).strip()
                            if hand_text:
                                yield hand_text
                            current_hand = []
                    
                    current_hand.append(line)


def stream_hands_from_combined_file(file_path: str, site: str) -> Iterator[str]:
    """
    Stream hands from a combined file using site-specific splitting
    
    Args:
        file_path: Path to combined hand history file
        site: Poker site name (pokerstars, gg, winamax, 888, wpn)
        
    Yields:
        Individual hand history text strings
    """
    streamer = HandStreamer(file_path, site=site)
    yield from streamer.stream_hands()


def count_hands_in_file(file_path: str, site: str) -> int:
    """
    Count total hands in file without loading all into memory
    
    Args:
        file_path: Path to hand history file  
        site: Poker site name
        
    Returns:
        Total number of hands
    """
    count = 0
    for _ in stream_hands_from_combined_file(file_path, site):
        count += 1
    return count
