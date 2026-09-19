import { StrictMode } from 'react'
import { createRoot } from 'react-dom/client'
import { App } from './App.tsx'
import { http } from './api.ts'
import './styles.css'

const root = document.getElementById('root')
if (root === null) throw new Error('index.html holds no element with the id root')

createRoot(root).render(
  <StrictMode>
    <App api={http} />
  </StrictMode>,
)
