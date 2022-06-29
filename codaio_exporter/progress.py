from typing import Callable

# Parameters: (doc_name, progress, progress_total)
ProgressCallback = Callable[[str, int, int], None]
