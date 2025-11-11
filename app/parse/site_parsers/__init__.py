# Site-specific parsers for different poker rooms
from .base_parser import BaseParser
from .site_detector import detect_poker_site, get_parser
from .gg_parser import GGPokerParser
from .pokerstars_parser import PokerStarsParser
from .wpn_parser import WPNParser
from .winamax_parser import WinamaxParser
from .party_parser import PartyPokerParser
from .eight88_parser import Eight88PokerParser

__all__ = [
    'BaseParser',
    'detect_poker_site',
    'get_parser',
    'GGPokerParser',
    'PokerStarsParser',
    'WPNParser',
    'WinamaxParser',
    'PartyPokerParser',
    'Eight88PokerParser'
]