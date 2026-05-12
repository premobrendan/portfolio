import { useState, useEffect } from 'react'
import PackList from './components/PackList'
import PackView from './components/PackView'
import CardBrowser from './components/CardBrowser'
import Quiz from './components/Quiz'
import RandomCard from './components/RandomCard'

function App() {
  const [packs, setPacks] = useState([])
  const [currentPack, setCurrentPack] = useState(null)
  const [view, setView] = useState('list') // list, pack, browse, quiz, random

  useEffect(() => {
    fetch('/info_packs/index.json')
      .then(res => res.json())
      .then(data => setPacks(data.packs))
      .catch(err => console.error('Failed to load packs:', err))
  }, [])

  const loadPack = async (packId) => {
    try {
      const res = await fetch(`/info_packs/${packId}/pack_config.json`)
      const packData = await res.json()
      setCurrentPack({ id: packId, ...packData })
      setView('pack')
    } catch (err) {
      console.error('Failed to load pack:', err)
    }
  }

  const goBack = () => {
    if (view === 'pack') {
      setView('list')
      setCurrentPack(null)
    } else {
      setView('pack')
    }
  }

  return (
    <div className="app">
      <h1>🎴 Flashcards</h1>

      {view === 'list' && (
        <PackList key="pack-list" packs={packs} onSelectPack={loadPack} />
      )}

      {view === 'pack' && currentPack && (
        <PackView
          pack={currentPack}
          onBack={goBack}
          onBrowse={() => setView('browse')}
          onQuiz={() => setView('quiz')}
          onRandom={() => setView('random')}
        />
      )}

      {view === 'browse' && currentPack && (
        <CardBrowser pack={currentPack} onBack={goBack} />
      )}

      {view === 'quiz' && currentPack && (
        <Quiz pack={currentPack} onBack={goBack} />
      )}

      {view === 'random' && currentPack && (
        <RandomCard pack={currentPack} onBack={goBack} />
      )}
    </div>
  )
}

export default App
