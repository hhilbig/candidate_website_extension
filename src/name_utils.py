"""
Shared name-parsing utilities for candidate roster building and URL lookups.

Extracted from build_candidate_roster.py so that multiple URL source modules
can reuse name cleaning, nickname extraction, and state name mapping.
"""

import re
from typing import Optional

# FEC nicknames: quoted strings preceded by whitespace (not mid-word apostrophes)
# Matches: CRUZ, RAFAEL EDWARD "TED" → TED
# Avoids: O'ROURKE (apostrophe is part of name, not a quote)
NICKNAME_PATTERN = re.compile(r'(?<=\s)["\']([A-Za-z]+)["\']')

# State abbreviation → full name mapping
STATE_NAMES = {
    "AL": "Alabama", "AK": "Alaska", "AZ": "Arizona", "AR": "Arkansas",
    "CA": "California", "CO": "Colorado", "CT": "Connecticut", "DE": "Delaware",
    "FL": "Florida", "GA": "Georgia", "HI": "Hawaii", "ID": "Idaho",
    "IL": "Illinois", "IN": "Indiana", "IA": "Iowa", "KS": "Kansas",
    "KY": "Kentucky", "LA": "Louisiana", "ME": "Maine", "MD": "Maryland",
    "MA": "Massachusetts", "MI": "Michigan", "MN": "Minnesota", "MS": "Mississippi",
    "MO": "Missouri", "MT": "Montana", "NE": "Nebraska", "NV": "Nevada",
    "NH": "New Hampshire", "NJ": "New Jersey", "NM": "New Mexico", "NY": "New York",
    "NC": "North Carolina", "ND": "North Dakota", "OH": "Ohio", "OK": "Oklahoma",
    "OR": "Oregon", "PA": "Pennsylvania", "RI": "Rhode Island", "SC": "South Carolina",
    "SD": "South Dakota", "TN": "Tennessee", "TX": "Texas", "UT": "Utah",
    "VT": "Vermont", "VA": "Virginia", "WA": "Washington", "WV": "West Virginia",
    "WI": "Wisconsin", "WY": "Wyoming", "DC": "District of Columbia",
    "AS": "American Samoa", "GU": "Guam", "MP": "Northern Mariana Islands",
    "PR": "Puerto Rico", "VI": "U.S. Virgin Islands",
}


def clean_name(raw) -> str:
    """Convert FEC name format to readable name.

    Strips quoted nicknames and extra whitespace.
    'CRUZ, RAFAEL EDWARD "TED"' → 'Rafael Edward Cruz'
    """
    if not raw or (hasattr(raw, '__class__') and raw.__class__.__name__ == 'float'):
        return ""
    raw = str(raw)
    # Remove quoted nicknames before parsing
    cleaned = re.sub(r'["\'][A-Za-z]+["\']', '', raw)
    # FEC: "LASTNAME, FIRSTNAME MIDDLE SUFFIX"
    parts = cleaned.split(",", 1)
    if len(parts) == 2:
        last = parts[0].strip().title()
        first = " ".join(parts[1].split()).strip().title()
        return f"{first} {last}"
    return " ".join(cleaned.split()).strip().title()


def extract_nickname(fec_name: str) -> Optional[str]:
    """Extract nickname from FEC name if present.

    FEC format: 'CRUZ, RAFAEL EDWARD "TED"' → "Ted"
    """
    match = NICKNAME_PATTERN.search(fec_name)
    if match:
        return match.group(1).strip().title()
    return None


def state_abbrev_to_name(abbrev: str) -> Optional[str]:
    """Convert 2-letter state abbreviation to full name."""
    return STATE_NAMES.get(abbrev.upper())


