import { useState } from 'react'

function FlashCard({ card }) {
  const [expandedDivisions, setExpandedDivisions] = useState({})

  const toggleDivision = (index) => {
    setExpandedDivisions(prev => ({
      ...prev,
      [index]: !prev[index]
    }))
  }

  const formatFactValue = (value) => {
    if (Array.isArray(value)) {
      return value.join(', ')
    }
    return value
  }

  const renderDivisions = () => {
    if (!card.divisions || card.divisions.length === 0) return null

    return (
      <div className="divisions-section">
        {card.divisions.map((division, index) => {
          const isExpanded = expandedDivisions[index]
          return (
            <div key={index} className="division-item">
              <button
                className="division-toggle"
                onClick={() => toggleDivision(index)}
              >
                <span className="division-arrow">{isExpanded ? '▼' : '▶'}</span>
                <span className="division-label">{division.label}</span>
                <span className="division-count">({division.list.length})</span>
              </button>
              {isExpanded && (
                <div className="division-list">
                  {division.list.join(', ')}
                </div>
              )}
            </div>
          )
        })}
      </div>
    )
  }

  return (
    <div className="flash-card">
      <div className="flag">{card.flag}</div>
      <h3>{card.name}</h3>
      <div className="capital">Capital: {card.capital}</div>

      {renderDivisions()}

      {card.facts && (
        <div className="facts-section">
          <h4>Key Facts</h4>
          {Object.entries(card.facts).map(([key, value]) => (
            <div key={key} className="fact-item">
              <strong>{key.replace(/_/g, ' ')}: </strong>
              <span>{formatFactValue(value)}</span>
            </div>
          ))}
        </div>
      )}
    </div>
  )
}

export default FlashCard
