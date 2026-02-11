from __future__ import annotations

from typing import Optional

from .store import Store


class FifoScheduler:
    def choose_next_task(self, store: Store) -> Optional[dict]:
        row = store.get_next_queued_task()
        if row is None:
            return None
        return dict(row)
