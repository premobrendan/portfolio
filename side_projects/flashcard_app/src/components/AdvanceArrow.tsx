/** The textless bottom-right advance control. Visible only once the card has been flipped. */
export default function AdvanceArrow({ onAdvance }: { onAdvance: () => void }) {
  return (
    <button
      type="button"
      className="advance"
      data-testid="advance-arrow"
      onClick={onAdvance}
      aria-label="Next card"
    >
      <svg width="22" height="22" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2">
        <line x1="5" y1="12" x2="19" y2="12" />
        <polyline points="12 5 19 12 12 19" />
      </svg>
    </button>
  )
}
