import { useState, useMemo } from 'react'
import FlashCard from './FlashCard'

function CardBrowser({ pack, onBack }) {
  const [selectedCard, setSelectedCard] = useState(null)
  const [expandedContinents, setExpandedContinents] = useState({})

  const cardsByContinent = useMemo(() => {
    const grouped = {}
    pack.cards.forEach(card => {
      const continent = card.facts?.continent || 'Other'
      if (!grouped[continent]) {
        grouped[continent] = []
      }
      grouped[continent].push(card)
    })
    // Sort cards within each continent alphabetically
    Object.keys(grouped).forEach(continent => {
      grouped[continent].sort((a, b) => a.name.localeCompare(b.name))
    })
    return grouped
  }, [pack.cards])

  const continents = useMemo(() => {
    return Object.keys(cardsByContinent).sort()
  }, [cardsByContinent])

  const toggleContinent = (continent) => {
    setExpandedContinents(prev => ({
      ...prev,
      [continent]: !prev[continent]
    }))
  }

  const handleCardClick = (card) => {
    setSelectedCard(card)
  }

  const handleBackToList = () => {
    setSelectedCard(null)
  }

  // Show individual card view
  if (selectedCard) {
    return (
      <div className="card-browser">
        <button className="back-btn" onClick={handleBackToList}>← Back to List</button>
        <FlashCard card={selectedCard} />
      </div>
    )
  }

  // Show continent listing
  return (
    <div className="card-browser">
      <button className="back-btn" onClick={onBack}>← Back</button>
      <h2>{pack.name}</h2>
      <p className="browser-subtitle">{pack.cards.length} cards organized by region</p>

      <div className="continent-list">
        {continents.map(continent => {
          const isExpanded = expandedContinents[continent]
          const cards = cardsByContinent[continent]

          return (
            <div key={continent} className="continent-section">
              <button
                className="continent-toggle"
                onClick={() => toggleContinent(continent)}
              >
                <span className="continent-arrow">{isExpanded ? '▼' : '▶'}</span>
                <span className="continent-name">{continent}</span>
                <span className="continent-count">({cards.length})</span>
              </button>

              {isExpanded && (
                <div className="country-list">
                  {cards.map(card => (
                    <button
                      key={card.id}
                      className="country-item"
                      onClick={() => handleCardClick(card)}
                    >
                      <span className="country-flag">{card.flag}</span>
                      <span className="country-name">{card.name}</span>
                      <span className="country-capital">{card.capital}</span>
                    </button>
                  ))}
                </div>
              )}
            </div>
          )
        })}
      </div>
    </div>
  )
}

export default CardBrowser
