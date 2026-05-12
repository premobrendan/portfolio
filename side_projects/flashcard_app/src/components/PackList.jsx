function PackList({ packs, onSelectPack }) {
  if (packs.length === 0) {
    return <p>Loading packs...</p>
  }

  const handleSelect = (packId) => {
    onSelectPack(packId)
  }

  return (
    <div className="pack-list">
      <h2>Select a Pack</h2>
      {packs.map(pack => (
        <button
          key={pack.id}
          className="pack-card"
          onClick={() => handleSelect(pack.id)}
          type="button"
        >
          <h3>{pack.name}</h3>
          <p>{pack.description}</p>
        </button>
      ))}
    </div>
  )
}

export default PackList
