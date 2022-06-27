import asyncio
import logging
from typing import List, TypeVar, Coroutine, Any

T = TypeVar('T')

# Like asyncio.gather(), but if one of the tasks fails, all others are cancelled instead of continuing to execute.
async def gather_cancel_on_first_error(*coroutines: Coroutine[Any, Any, T]) -> List[T]:
    try:
        futures = [asyncio.create_task(c) for c in coroutines]
        return await asyncio.gather(*futures)
    except:
        for future in futures:
            future.cancel()
        raise

# Like asyncio.gather(), but if one of the tasks failed, first all other tasks are completed,
# then we log all errors and raise the first error.
async def gather_raise_first_error_after_all_tasks_complete(*coroutines: Coroutine[Any, Any, T]) -> List[T]:
    futures = [asyncio.create_task(c) for c in coroutines]
    results = await asyncio.gather(*futures, return_exceptions=True)
    errors = [result for result in results if isinstance(result, Exception)]
    for error in errors:
        logging.error(error)
    if len(errors) > 0:
        raise errors[0]
    
    return results
