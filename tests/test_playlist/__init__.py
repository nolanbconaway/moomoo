import uuid
from pathlib import Path
from typing import List

import pytest

from moomoo import utils_
from moomoo.playlist.playlist_generator import NoFilesRequestedError, PlaylistGenerator
