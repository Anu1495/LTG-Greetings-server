const fs = require('fs')
const path = require('path')
require('dotenv').config({ path: path.join(__dirname, '..', '.env') })

const BIRD_API_KEY = process.env.BIRD_API_KEY
const WORKSPACE = process.env.BIRD_WORKSPACE_ID
const CHANNEL = process.env.BIRD_CHANNEL_ID

if(!BIRD_API_KEY || !WORKSPACE || !CHANNEL){
  console.error('Missing BIRD_API_KEY, BIRD_WORKSPACE_ID or BIRD_CHANNEL_ID in .env')
  process.exit(2)
}

const phone = process.argv[2] || '+34630929503'

async function fetchMessages(){
  const url = `https://api.bird.com/workspaces/${WORKSPACE}/channels/${CHANNEL}/messages?limit=500`
  const res = await fetch(url, { headers: { Authorization: `AccessKey ${BIRD_API_KEY}`, Accept: 'application/json' } })
  if(!res.ok){
    const txt = await res.text().catch(()=>'<no body>')
    throw new Error(`Bird API returned ${res.status}: ${txt.slice(0,200)}`)
  }
  const j = await res.json()
  return j.results || []
}

function normalize(m){
  let text = ''
  let image = null
  try{
    if(m.body){
      if(m.body.text && m.body.text.text) text = m.body.text.text
      else if(m.body.list && m.body.list.text) text = m.body.list.text
      else if(m.body.image){
        if(m.body.image.text) text = m.body.image.text
        const url = m.body.image.url || m.body.image.src
          || (m.body.image.media && m.body.image.media[0] && m.body.image.media[0].url)
          || m.body.image.mediaUrl
          || (m.body.image.images && m.body.image.images[0] && (m.body.image.images[0].mediaUrl || m.body.image.images[0].url || m.body.image.images[0].src))
        if(url) image = { url, text: text || '' }
      } else text = JSON.stringify(m.body)
    }
  }catch(e){ /* ignore */ }
  const body = image ? { image } : { text }
  return {
    id: m.id,
    direction: m.direction || (m.sender && m.sender.connector ? 'outgoing' : 'incoming'),
    createdAt: m.createdAt,
    body,
    raw: m
  }
}

;(async ()=>{
  try{
    const msgs = await fetchMessages()
    const filtered = msgs.filter(m=>{
      try{
        const rcv = m.receiver && m.receiver.contacts && m.receiver.contacts[0] && m.receiver.contacts[0].identifierValue
        const snd = m.sender && m.sender.contact && m.sender.contact.identifierValue
        return [rcv,snd].includes(phone)
      }catch(e){return false}
    })
    const normalized = filtered.map(normalize).sort((a,b)=> new Date(a.createdAt) - new Date(b.createdAt))
    const out = JSON.stringify(normalized, null, 2)
    const fp = path.join(__dirname, 'inspect_output.json')
    fs.writeFileSync(fp, out)
    console.log('Wrote', fp, 'entries:', normalized.length)
    process.stdout.write(out)
  }catch(e){
    console.error('Error:', e && e.message || e)
    process.exit(1)
  }
})()
