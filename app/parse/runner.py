"""
Main orchestrator for parsing poker hand histories.
Coordinates site detection and parsing across multiple formats.
"""

import json
import logging
import argparse
import sys
from typing import List, Dict, Optional, Union
from pathlib import Path
from datetime import datetime

from .schemas import Hand
from .site_pokerstars import PokerStarsParser
from .site_gg import GGParser
from .site_wpn import WPNParser
from .site_winamax import WinamaxParser
from .site_888 import Poker888Parser
from .site_generic import GenericParser

logger = logging.getLogger(__name__)


class ParserRunner:
    """Orchestrates parsing across different poker sites."""
    
    def __init__(self, hero_aliases_path: Optional[Path] = None):
        """
        Initialize the parser runner.
        
        Args:
            hero_aliases_path: Path to hero_aliases.json config file
        """
        # Load hero aliases configuration
        self.hero_aliases = self._load_hero_aliases(hero_aliases_path)
        
        # Initialize all parsers
        self.parsers = [
            PokerStarsParser(),
            GGParser(),
            WPNParser(),
            WinamaxParser(),
            Poker888Parser(),
            GenericParser()  # Must be last as fallback
        ]
    
    def _load_hero_aliases(self, path: Optional[Path]) -> Dict[str, List[str]]:
        """Load hero aliases from configuration file."""
        if path and path.exists():
            try:
                with open(path, 'r', encoding='utf-8') as f:
                    return json.load(f)
            except Exception as e:
                logger.warning(f"Failed to load hero aliases from {path}: {e}")
        
        # Default empty configuration
        return {
            'global': [],
            'pokerstars': [],
            'gg': [],
            'wpn': [],
            'winamax': [],
            '888': []
        }
    
    def detect_site(self, text: str) -> Optional[str]:
        """
        Detect which poker site format the text uses.
        
        Returns:
            Site name ('pokerstars', 'gg', etc.) or 'other' for unknown
        """
        for parser in self.parsers[:-1]:  # Skip generic parser
            if parser.detect(text):
                return parser.__class__.__name__.replace('Parser', '').lower()
        
        return 'other'  # Generic/unknown format
    
    def parse_file(self, file_path: Union[str, Path]) -> List[Hand]:
        """
        Parse a single file containing hand histories.
        
        Args:
            file_path: Path to the file to parse
            
        Returns:
            List of parsed Hand objects
        """
        file_path = Path(file_path)
        
        if not file_path.exists():
            logger.error(f"File not found: {file_path}")
            return []
        
        try:
            with open(file_path, 'r', encoding='utf-8', errors='replace') as f:
                text = f.read()
        except Exception as e:
            logger.error(f"Failed to read file {file_path}: {e}")
            return []
        
        return self.parse_text(text, file_id=file_path.name)
    
    def parse_text(self, text: str, file_id: str = 'unknown') -> List[Hand]:
        """
        Parse hand history text.
        
        Args:
            text: Raw hand history text
            file_id: Identifier for this file/text
            
        Returns:
            List of parsed Hand objects
        """
        # Try each parser until one works
        for parser in self.parsers:
            if parser.detect(text):
                logger.info(f"Using {parser.__class__.__name__} for file {file_id}")
                try:
                    hands = parser.parse_tournament(text, file_id, self.hero_aliases)
                    logger.info(f"Parsed {len(hands)} hands from {file_id}")
                    return hands
                except Exception as e:
                    logger.error(f"Parser {parser.__class__.__name__} failed on {file_id}: {e}")
                    # Continue to next parser
        
        logger.warning(f"No parser could handle file {file_id}")
        return []
    
    def parse_directory(
        self,
        directory: Union[str, Path],
        extensions: List[str] = ['.txt', '.xml']
    ) -> Dict[str, List[Hand]]:
        """
        Parse all files in a directory.
        
        Args:
            directory: Directory containing hand history files
            extensions: File extensions to process
            
        Returns:
            Dictionary mapping file names to lists of hands
        """
        directory = Path(directory)
        
        if not directory.exists():
            logger.error(f"Directory not found: {directory}")
            return {}
        
        results = {}
        
        # Process all matching files
        for ext in extensions:
            for file_path in directory.glob(f'*{ext}'):
                if file_path.is_file():
                    logger.info(f"Processing {file_path}")
                    hands = self.parse_file(file_path)
                    if hands:
                        results[file_path.name] = hands
        
        logger.info(f"Parsed {len(results)} files from {directory}")
        return results


    def parse_folder(
        self,
        in_root: str,
        out_jsonl: str,
        hero_aliases_path: Optional[str] = None
    ) -> Dict:
        """
        Parse all files in classified folders and write to JSONL.
        
        Traverses PKO/, non-KO/, mystery/ subfolders (if they exist),
        auto-detects site per file, parses tournaments, and writes
        all hands to a single JSONL file (one hand per line).
        
        Args:
            in_root: Root directory containing classified folders
            out_jsonl: Output JSONL file path
            hero_aliases_path: Optional path to hero aliases config
            
        Returns:
            Summary dict with file count, hand count, and breakdown by site
        """
        in_root = Path(in_root)
        out_jsonl = Path(out_jsonl)
        
        # Load hero aliases if provided
        if hero_aliases_path:
            self.hero_aliases = self._load_hero_aliases(Path(hero_aliases_path))
        
        # Ensure output directory exists
        out_jsonl.parent.mkdir(parents=True, exist_ok=True)
        
        # Open error log file
        error_log_path = out_jsonl.parent / 'parse_errors.log'
        error_log = open(error_log_path, 'w', encoding='utf-8')
        
        # Statistics
        stats = {
            "files": 0,
            "hands": 0,
            "by_site": {},
            "by_folder": {},
            "errors": []
        }
        
        # Folders to check
        folders_to_check = ['PKO', 'non-KO', 'NON-KO', 'mystery', 'mysteries', 'MYSTERIES']
        
        # Open JSONL file for writing
        with open(out_jsonl, 'w', encoding='utf-8') as jsonl_file:
            # Process each folder
            for folder_name in folders_to_check:
                folder_path = in_root / folder_name
                if not folder_path.exists():
                    continue
                
                logger.info(f"Processing folder: {folder_name}")
                folder_hands = 0
                
                # Collect files for parallel processing
                files_to_process = []
                for file_path in folder_path.glob('*'):
                    if file_path.is_file() and file_path.suffix.lower() in ['.txt', '.xml']:
                        files_to_process.append(file_path)
                
                # Process files in batches for better memory management
                batch_size = min(10, len(files_to_process))  # Process max 10 files at once
                
                for i in range(0, len(files_to_process), batch_size):
                    batch = files_to_process[i:i + batch_size]
                    logger.info(f"Processing batch {i//batch_size + 1}/{(len(files_to_process) + batch_size - 1)//batch_size}")
                    
                    for file_path in batch:
                        stats["files"] += 1
                        
                        try:
                            # Parse file with size check for optimization
                            file_size = file_path.stat().st_size
                            if file_size > 50 * 1024 * 1024:  # 50MB+
                                logger.info(f"Large file detected: {file_path.name} ({file_size / 1024 / 1024:.1f}MB)")
                            
                            hands = self.parse_file(file_path)
                        except Exception as e:
                            logger.error(f"Error parsing file {file_path}: {e}")
                            hands = []
                        
                        # File statistics
                        file_stats = {
                            'total': len(hands),
                            'allin_preflop': 0,
                            'hu_flop': 0,
                            'mw_flop': 0,
                            'total_dealt_in': 0,
                            'valid_hands': 0,
                            'critical_errors': []
                        }
                        
                        # Detect site for statistics
                        if hands:
                            site = hands[0].site
                            stats["by_site"][site] = stats["by_site"].get(site, 0) + len(hands)
                        
                        # Process and validate each hand
                        for hand_idx, hand in enumerate(hands):
                            # Check for critical errors
                            has_critical_error = False
                            errors = []
                            
                            if hand.button_seat is None:
                                errors.append("missing button")
                                has_critical_error = True
                            
                            if not hand.streets.get('preflop') or not hand.streets['preflop'].actions:
                                errors.append("no preflop actions")
                                has_critical_error = True
                            
                            if has_critical_error:
                                # Log critical error
                                offset_info = f"offsets={hand.raw_offsets}" if hand.raw_offsets else "no offsets"
                                error_entry = f"{datetime.now().isoformat()} - {file_path.name} - Hand #{hand_idx} - Errors: {', '.join(errors)} - {offset_info}\n"
                                error_log.write(error_entry)
                                file_stats['critical_errors'].append(error_entry.strip())
                            else:
                                # Valid hand - collect stats
                                file_stats['valid_hands'] += 1
                                
                                if hand.any_allin_preflop:
                                    file_stats['allin_preflop'] += 1
                                
                                if hand.heads_up_flop:
                                    file_stats['hu_flop'] += 1
                                elif hand.players_to_flop > 2:
                                    file_stats['mw_flop'] += 1
                                
                                file_stats['total_dealt_in'] += len(hand.players_dealt_in)
                            
                            # Write hand to JSONL (even if has errors for analysis)
                            json_line = hand.model_dump_json(exclude_none=True)
                            jsonl_file.write(json_line + '\n')
                            stats["hands"] += 1
                            folder_hands += 1
                        
                        # Log file statistics
                        avg_dealt_in = file_stats['total_dealt_in'] / file_stats['valid_hands'] if file_stats['valid_hands'] > 0 else 0
                        allin_pct = (file_stats['allin_preflop'] / file_stats['valid_hands'] * 100) if file_stats['valid_hands'] > 0 else 0
                        hu_pct = (file_stats['hu_flop'] / file_stats['valid_hands'] * 100) if file_stats['valid_hands'] > 0 else 0
                        mw_pct = (file_stats['mw_flop'] / file_stats['valid_hands'] * 100) if file_stats['valid_hands'] > 0 else 0
                        
                        logger.info(f"  {file_path.name}: {file_stats['total']} hands (valid: {file_stats['valid_hands']}, errors: {len(file_stats['critical_errors'])})")
                        logger.info(f"    All-in preflop: {allin_pct:.1f}% | Flop: HU {hu_pct:.1f}%, MW {mw_pct:.1f}% | Avg dealt-in: {avg_dealt_in:.1f}")
                        
                        if file_stats['critical_errors']:
                            logger.warning(f"    Critical errors: {len(file_stats['critical_errors'])} hands with missing data")
                
                if folder_hands > 0:
                    stats["by_folder"][folder_name] = folder_hands
        
        # Close error log
        error_log.close()
        
        # Add timestamp and output info
        stats["timestamp"] = datetime.now().isoformat()
        stats["output_file"] = str(out_jsonl)
        stats["error_log"] = str(error_log_path)
        
        logger.info(f"Parsing complete: {stats['files']} files, {stats['hands']} hands")
        logger.info(f"Error log saved to: {error_log_path}")
        return stats


# Convenience functions for module-level usage
_default_runner = None


def reset_default_runner() -> None:
    """Reset the default runner to force re-instantiation with updated parsers."""
    global _default_runner
    _default_runner = None


def get_default_runner(reset: bool = False) -> ParserRunner:
    """Get or create the default parser runner."""
    global _default_runner
    if reset:
        _default_runner = None
    if _default_runner is None:
        # Try to load config from standard location
        config_path = Path(__file__).parent.parent / 'config' / 'hero_aliases.json'
        _default_runner = ParserRunner(config_path)
    return _default_runner


def parse_file(file_path: Union[str, Path]) -> List[Hand]:
    """
    Parse a single file using the default runner.
    
    Args:
        file_path: Path to file to parse
        
    Returns:
        List of parsed Hand objects
    """
    return get_default_runner().parse_file(file_path)


def parse_directory(
    directory: Union[str, Path],
    extensions: List[str] = ['.txt', '.xml']
) -> Dict[str, List[Hand]]:
    """
    Parse all files in a directory using the default runner.
    
    Args:
        directory: Directory to process
        extensions: File extensions to include
        
    Returns:
        Dictionary mapping file names to lists of hands
    """
    return get_default_runner().parse_directory(directory, extensions)


def parse_folder(in_root: str, out_jsonl: str, hero_aliases_path: str) -> Dict:
    """
    Parse all files in classified folders and write to JSONL.
    
    Convenience function for CLI and external usage.
    
    Args:
        in_root: Root directory containing PKO/, non-KO/, mystery/ folders
        out_jsonl: Output JSONL file path
        hero_aliases_path: Path to hero aliases JSON config
        
    Returns:
        Summary with file count, hand count, and breakdown by site
    """
    runner = ParserRunner(Path(hero_aliases_path) if hero_aliases_path else None)
    return runner.parse_folder(in_root, out_jsonl, hero_aliases_path)


# CLI interface
def main():
    """CLI entry point for parser runner."""
    parser = argparse.ArgumentParser(
        description='Parse poker hand histories from classified folders',
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    
    parser.add_argument(
        '--in',
        dest='input_dir',
        required=True,
        help='Input directory containing PKO/, non-KO/, mystery/ folders'
    )
    
    parser.add_argument(
        '--out',
        dest='output_file',
        required=True,
        help='Output JSONL file path'
    )
    
    parser.add_argument(
        '--aliases',
        dest='aliases_path',
        default='./app/config/hero_aliases.json',
        help='Path to hero aliases JSON config (default: ./app/config/hero_aliases.json)'
    )
    
    parser.add_argument(
        '--verbose',
        action='store_true',
        help='Enable verbose logging'
    )
    
    args = parser.parse_args()
    
    # Configure logging
    log_level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(
        level=log_level,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Parse folder
    try:
        stats = parse_folder(
            in_root=args.input_dir,
            out_jsonl=args.output_file,
            hero_aliases_path=args.aliases_path
        )
        
        # Print summary
        print("\n=== Parsing Complete ===")
        print(f"Files processed: {stats['files']}")
        print(f"Hands extracted: {stats['hands']}")
        print(f"Output: {stats['output_file']}")
        
        if stats['by_site']:
            print("\nBy site:")
            for site, count in stats['by_site'].items():
                print(f"  {site}: {count} hands")
        
        if stats['by_folder']:
            print("\nBy folder:")
            for folder, count in stats['by_folder'].items():
                print(f"  {folder}: {count} hands")
        
        if stats['errors']:
            print(f"\nErrors: {len(stats['errors'])}")
            for error in stats['errors'][:5]:  # Show first 5 errors
                print(f"  - {error}")
        
        sys.exit(0)
        
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == '__main__':
    main()