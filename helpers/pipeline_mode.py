from enum import Enum


class PipelineMode(Enum):
    NEW_PATRONS = 1
    UPDATED_PATRONS = 2
    DELETED_PATRONS = 3

    def __str__(self):
        return self.name.lower().rstrip('_patrons')
