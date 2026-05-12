import { useState, useMemo } from 'react'

function Quiz({ pack, onBack }) {
  const [selectedMode, setSelectedMode] = useState(null)
  const [currentIndex, setCurrentIndex] = useState(0)
  const [userAnswer, setUserAnswer] = useState('')
  const [showFeedback, setShowFeedback] = useState(false)
  const [isCorrect, setIsCorrect] = useState(false)
  const [score, setScore] = useState(0)
  const [quizComplete, setQuizComplete] = useState(false)

  const shuffledCards = useMemo(() => {
    return [...pack.cards].sort(() => Math.random() - 0.5)
  }, [pack.cards, selectedMode])

  const currentCard = shuffledCards[currentIndex]

  const handleModeSelect = (mode) => {
    setSelectedMode(mode)
    setCurrentIndex(0)
    setScore(0)
    setQuizComplete(false)
  }

  const checkAnswer = () => {
    if (!currentCard || !selectedMode) return

    const correctAnswer = currentCard[selectedMode.answer_field]
    const correct = userAnswer.toLowerCase().trim() === correctAnswer.toLowerCase().trim()

    setIsCorrect(correct)
    if (correct) setScore(s => s + 1)
    setShowFeedback(true)
  }

  const nextQuestion = () => {
    if (currentIndex + 1 >= shuffledCards.length) {
      setQuizComplete(true)
    } else {
      setCurrentIndex(i => i + 1)
      setUserAnswer('')
      setShowFeedback(false)
    }
  }

  const restartQuiz = () => {
    setSelectedMode(null)
    setCurrentIndex(0)
    setUserAnswer('')
    setShowFeedback(false)
    setScore(0)
    setQuizComplete(false)
  }

  const handleKeyDown = (e) => {
    if (e.key === 'Enter') {
      if (showFeedback) {
        nextQuestion()
      } else if (userAnswer.trim()) {
        checkAnswer()
      }
    }
  }

  // Mode selection
  if (!selectedMode) {
    return (
      <div className="quiz">
        <button className="back-btn" onClick={onBack}>← Back</button>
        <h2>Select Quiz Mode</h2>
        <div className="mode-select">
          {pack.quiz_modes.map(mode => (
            <button
              key={mode.id}
              className="mode-btn"
              onClick={() => handleModeSelect(mode)}
            >
              {mode.label}
            </button>
          ))}
        </div>
      </div>
    )
  }

  // Quiz complete
  if (quizComplete) {
    return (
      <div className="quiz">
        <button className="back-btn" onClick={onBack}>← Back</button>
        <div className="quiz-results">
          <h2>Quiz Complete!</h2>
          <div className="score">{score}/{shuffledCards.length}</div>
          <p>{Math.round((score / shuffledCards.length) * 100)}% correct</p>
          <button className="restart-btn" onClick={restartQuiz}>
            Try Again
          </button>
        </div>
      </div>
    )
  }

  // Quiz in progress
  const questionValue = currentCard[selectedMode.question_field]
  const isFlag = selectedMode.question_field === 'flag'

  return (
    <div className="quiz">
      <button className="back-btn" onClick={onBack}>← Back</button>
      <div className="quiz-card">
        <div className="quiz-progress">
          Question {currentIndex + 1} of {shuffledCards.length}
        </div>

        <div className="quiz-question">
          {isFlag ? (
            <>
              <span>Which country has this flag?</span>
              <span className="flag-display">{questionValue}</span>
            </>
          ) : (
            selectedMode.prompt.replace(`{${selectedMode.question_field}}`, questionValue)
          )}
        </div>

        <input
          type="text"
          className="quiz-input"
          value={userAnswer}
          onChange={(e) => setUserAnswer(e.target.value)}
          onKeyDown={handleKeyDown}
          placeholder="Type your answer..."
          disabled={showFeedback}
          autoFocus
        />

        {!showFeedback ? (
          <button
            className="submit-btn"
            onClick={checkAnswer}
            disabled={!userAnswer.trim()}
          >
            Submit
          </button>
        ) : (
          <>
            <div className={`feedback ${isCorrect ? 'correct' : 'incorrect'}`}>
              {isCorrect ? (
                '✓ Correct!'
              ) : (
                <>✗ Incorrect. The answer is: {currentCard[selectedMode.answer_field]}</>
              )}
            </div>
            <button className="next-btn" onClick={nextQuestion}>
              {currentIndex + 1 >= shuffledCards.length ? 'See Results' : 'Next Question'}
            </button>
          </>
        )}
      </div>
    </div>
  )
}

export default Quiz
