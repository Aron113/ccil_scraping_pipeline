from pydantic import BaseModel, ValidationError
import time

class CCILRecord(BaseModel):
    """
    Model representing a CCIL record.
    """
    ismt_idnt: str
    ttc: int
    tta: str
    op: str
    hi: str
    lo: str
    ltp: str
    arrow: str
    indicator: str
    lty: float
    prev_trad_rate: float
    trade_yeild: float
    mrkt_indc: str
    book_indc: str

def retry_on_failure(func, retries=3, delay=5):
    """
    Retry decorator to handle failures.

    Args:
        func (callable): The function to retry.
        retries (int): Number of retries.
        delay (int): Delay between retries in seconds.
    """
    def wrapper(*args, **kwargs):
        for attempt in range(retries):
            try:
                return func(*args, **kwargs)
            except Exception as e:
                if attempt < retries - 1:
                    print(f"Attempt {attempt + 1} failed: {e}. Retrying in {delay} seconds...")
                    time.sleep(delay)
                else:
                    print(f"All attempts failed: {e}")
    return wrapper
