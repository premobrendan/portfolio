import { useState, useCallback } from 'react'
import FlashCard from './FlashCard'

function RandomCard({ pack, onBack }) {
  const getRandomCard = useCallback(() => {
    const randomIndex = Math.floor(Math.random() * pack.cards.length)
    return pack.cards[randomIndex]
  }, [pack.cards])

  const [currentCard, setCurrentCard] = useState(getRandomCard)

  const handleReroll = () => {
    setCurrentCard(getRandomCard())
  }

  return (
    <div className="random-card-view">
      <button className="back-btn" onClick={onBack}>← Back</button>
      <div className="random-card-header">
        <h2>Random Card</h2>
        <button className="reroll-btn" onClick={handleReroll}>
          🎲 Reroll
        </button>
      </div>
      <FlashCard card={currentCard} />
    </div>
  )
}

export default RandomCard
