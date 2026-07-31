import random
import statistics
import time
from pathlib import Path
from typing import Optional

STRINGS_FILE = Path(__file__).parent / "palindrome_test_strings.txt"
MONTE_CARLO_ITERATIONS = 10_000
MONTE_CARLO_MIN_LENGTH = 10
MONTE_CARLO_MAX_LENGTH = 1_000
# Each iteration rolls 1-100: above this value -> palindrome, at or below -> non-palindrome.
MONTE_CARLO_PALINDROME_THRESHOLD = 95


def pal_1(word):
    chars = [char.upper() for char in word]
    for i in range((len(chars) // 2) + 1):
        if chars[i] != chars[(-1 * i) - 1]:
            return False
    return True
    

def pal_2(word):
    chars = [char.upper() for char in word]
    chars_backward = chars[::-1]
    return True if chars_backward == chars else False


def make_palindrome(n: int) -> str:
    half = "a" * (n // 2)
    if n % 2:
        return half + "b" + half[::-1]
    return half + half[::-1]


def make_non_palindrome(n: int, rng: Optional[random.Random] = None) -> tuple[str, int]:
    """Return a non-palindrome and the index where the bad character was inserted."""
    s = make_palindrome(n)
    rng = rng or random.Random()
    # Avoid the center index on odd-length strings — flipping it still leaves a palindrome.
    center = n // 2 if n % 2 else None
    idx = rng.randrange(n - 1 if center is not None else n)
    if center is not None and idx >= center:
        idx += 1
    flipped = "z" if s[idx].lower() != "z" else "y"
    return s[:idx] + flipped + s[idx + 1 :], idx


def generate_test_strings(
    path: Path = STRINGS_FILE, length: int = 100_000, seed: int = 42
) -> Path:
    rng = random.Random(seed)
    palindrome = make_palindrome(length)
    non_palindrome, flip_idx = make_non_palindrome(length, rng)

    path.write_text(
        f"PALINDROME_LENGTH={len(palindrome)}\n"
        f"NON_PALINDROME_LENGTH={len(non_palindrome)}\n"
        f"NON_PALINDROME_FLIP_INDEX={flip_idx}\n"
        f"---PALINDROME---\n{palindrome}\n"
        f"---NON_PALINDROME---\n{non_palindrome}\n",
        encoding="utf-8",
    )
    print(f"Wrote {path} ({path.stat().st_size:,} bytes, flip at index {flip_idx:,})")
    return path


def time_once(fn, word: str) -> float:
    start = time.perf_counter()
    fn(word)
    return time.perf_counter() - start


def _summarize_times(label: str, times: list[float]) -> None:
    if not times:
        print(f"  {label}: no samples")
        return
    ms = [t * 1000 for t in times]
    print(
        f"  {label}: mean {statistics.mean(ms):,.2f} ms | "
        f"median {statistics.median(ms):,.2f} ms | "
        f"min {min(ms):,.2f} ms | max {max(ms):,.2f} ms"
    )


def run_monte_carlo(
    iterations: int = MONTE_CARLO_ITERATIONS,
    min_length: int = MONTE_CARLO_MIN_LENGTH,
    max_length: int = MONTE_CARLO_MAX_LENGTH,
    palindrome_threshold: int = MONTE_CARLO_PALINDROME_THRESHOLD,
    seed: int = 42,
) -> None:
    rng = random.Random(seed)

    pal1_on_palindrome: list[float] = []
    pal2_on_palindrome: list[float] = []
    pal1_on_non_palindrome: list[float] = []
    pal2_on_non_palindrome: list[float] = []
    flip_positions: list[int] = []
    string_lengths: list[int] = []
    pal1_wins_palindrome = 0
    pal1_wins_non_palindrome = 0

    expected_palindrome_pct = 100 - palindrome_threshold
    print(
        f"Monte Carlo: {iterations:,} iterations, random length "
        f"{min_length:,}-{max_length:,} chars (seed={seed})"
    )
    print(
        f"Roll 1-100 per iteration: >{palindrome_threshold} -> palindrome "
        f"(~{expected_palindrome_pct}%), <={palindrome_threshold} -> non-palindrome "
        f"(~{100 - expected_palindrome_pct}%)"
    )
    started = time.perf_counter()

    for _ in range(iterations):
        length = rng.randint(min_length, max_length)
        string_lengths.append(length)
        roll = rng.randint(1, 100)

        if roll > palindrome_threshold:
            word = make_palindrome(length)

            t1 = time_once(pal_1, word)
            t2 = time_once(pal_2, word)
            pal1_on_palindrome.append(t1)
            pal2_on_palindrome.append(t2)
            if t1 < t2:
                pal1_wins_palindrome += 1

            assert pal_1(word)
            assert pal_2(word)
        else:
            word, flip_idx = make_non_palindrome(length, rng)
            flip_positions.append(flip_idx)

            t1 = time_once(pal_1, word)
            t2 = time_once(pal_2, word)
            pal1_on_non_palindrome.append(t1)
            pal2_on_non_palindrome.append(t2)
            if t1 < t2:
                pal1_wins_non_palindrome += 1

            assert not pal_1(word)
            assert not pal_2(word)

    elapsed = time.perf_counter() - started
    palindrome_rolls = len(pal1_on_palindrome)
    non_palindrome_rolls = len(pal1_on_non_palindrome)

    print(f"\nCompleted in {elapsed:,.1f}s")
    print(
        f"String length: mean {statistics.mean(string_lengths):,.0f} | "
        f"median {statistics.median(string_lengths):,.0f} | "
        f"min {min(string_lengths):,} | max {max(string_lengths):,}"
    )
    print(
        f"Roll outcome: palindrome {palindrome_rolls:,} "
        f"({100 * palindrome_rolls / iterations:.1f}%) | "
        f"non-palindrome {non_palindrome_rolls:,} "
        f"({100 * non_palindrome_rolls / iterations:.1f}%)"
    )
    if flip_positions:
        print(
            f"Random flip index: mean {statistics.mean(flip_positions):,.0f} | "
            f"median {statistics.median(flip_positions):,.0f}"
        )

    print(
        f"\nPalindrome strings ({palindrome_rolls:,} rolls, "
        f"both functions scan the full string):"
    )
    _summarize_times("pal_1", pal1_on_palindrome)
    _summarize_times("pal_2", pal2_on_palindrome)
    if palindrome_rolls:
        print(
            f"  pal_1 faster: {pal1_wins_palindrome:,}/{palindrome_rolls:,} "
            f"({100 * pal1_wins_palindrome / palindrome_rolls:.1f}%)"
        )

    print(
        f"\nNon-palindrome strings ({non_palindrome_rolls:,} rolls, "
        f"bad char at random index):"
    )
    _summarize_times("pal_1", pal1_on_non_palindrome)
    _summarize_times("pal_2", pal2_on_non_palindrome)
    if non_palindrome_rolls:
        print(
            f"  pal_1 faster: {pal1_wins_non_palindrome:,}/{non_palindrome_rolls:,} "
            f"({100 * pal1_wins_non_palindrome / non_palindrome_rolls:.1f}%)"
        )

    print("\nOverall averages:")
    for label, times1, times2 in (
        ("palindrome    ", pal1_on_palindrome, pal2_on_palindrome),
        ("non-palindrome", pal1_on_non_palindrome, pal2_on_non_palindrome),
    ):
        if not times1:
            print(f"  {label}: no samples")
            continue
        avg1 = statistics.mean(times1)
        avg2 = statistics.mean(times2)
        print(
            f"  {label}: pal_1 {avg1 * 1000:,.2f} ms vs "
            f"pal_2 {avg2 * 1000:,.2f} ms "
            f"({max(avg1, avg2) / min(avg1, avg2):.2f}x)"
        )


if __name__ == "__main__":
    run_monte_carlo()
