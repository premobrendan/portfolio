function PackView({ pack, onBack, onBrowse, onQuiz, onRandom }) {
  return (
    <div className="pack-view">
      <button className="back-btn" onClick={onBack}>← Back to Packs</button>
      <h2>{pack.name}</h2>
      <p style={{ color: '#888', marginBottom: '1.5rem' }}>
        {pack.cards.length} card{pack.cards.length !== 1 ? 's' : ''}
      </p>
      <div className="pack-actions">
        <button className="browse-btn" onClick={onBrowse}>
          📖 Browse Cards
        </button>
        <button className="quiz-btn" onClick={onQuiz}>
          🎯 Start Quiz
        </button>
        <button className="random-btn" onClick={onRandom}>
          🎲 Random Card
        </button>
      </div>
    </div>
  )
}

export default PackView
