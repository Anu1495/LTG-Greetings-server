const fs = require('fs')
const path = require('path')
const dotenv = require('dotenv')

// Load .env from workspace root (one level up from server)
dotenv.config({ path: path.join(__dirname, '..', '.env') })

const BIRD_API_KEY = process.env.BIRD_API_KEY
if(!BIRD_API_KEY){
  console.error('BIRD_API_KEY not found in .env')
  process.exit(2)
}

const url = process.argv[2] || 'https://media.api.bird.com/workspaces/b4570c5b-32fc-48d8-a9c2-8d500638de7e/messages/8b7d4e9e-630d-4022-977b-02a4f0ca933d/media/849647a7-a999-47fc-9bb2-09ac772f1424'

async function run(){
  try{
    console.log('Fetching', url)
    const res = await fetch(url, { headers: { Authorization: `AccessKey ${BIRD_API_KEY}` }, redirect: 'follow' })
    console.log('Status:', res.status)
    for(const [k,v] of res.headers){
      if(['content-type','content-length','content-disposition'].includes(k)) console.log(k+':', v)
    }
    if(!res.ok){
      const txt = await res.text().catch(()=>'<no body>')
      console.error('Remote responded with non-OK status. Body snippet:', txt.slice(0,200))
      process.exit(3)
    }
    const arrayBuffer = await res.arrayBuffer()
    const buf = Buffer.from(arrayBuffer)
    const out = path.join(__dirname, 'bird_test.jpg')
    fs.writeFileSync(out, buf)
    console.log('Saved to', out, 'size', buf.length)
  }catch(e){
    console.error('Fetch failed', e)
    process.exit(1)
  }
}

run()
