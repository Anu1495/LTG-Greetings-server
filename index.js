const express = require('express')
const fs = require('fs')
const path = require('path')
const cors = require('cors')
const crypto = require('crypto')
const { parse } = require('csv-parse/sync')
// Prefer global fetch (Node 18+); fall back to node-fetch with default interop
let fetch = global.fetch
if (!fetch) {
  try {
    const nf = require('node-fetch')
    fetch = nf && nf.default ? nf.default : nf
  } catch (e) {
    fetch = null
  }
}
// Load environment from workspace root if present
const dotenv = require('dotenv')
const envPathRoot = path.join(process.cwd(),'..','.env')
if (fs.existsSync(envPathRoot)) {
  dotenv.config({ path: envPathRoot })
} else {
  dotenv.config()
}

const app = express()
app.use(cors())
app.use(express.json())
// serve uploaded files
const uploadsDir = path.join(process.cwd(), 'uploads')
if(!fs.existsSync(uploadsDir)) fs.mkdirSync(uploadsDir, { recursive: true })
app.use('/uploads', express.static(uploadsDir))

// multer for handling file uploads
const multer = require('multer')
const storage = multer.diskStorage({
  destination: function (req, file, cb) { cb(null, uploadsDir) },
  filename: function (req, file, cb) {
    // keep original name with timestamp prefix to avoid collisions
    const safe = (Date.now() + '-' + file.originalname).replace(/[^a-zA-Z0-9.\-\_]/g, '_')
    cb(null, safe)
  }
})
const upload = multer({ storage })

// Helper function to check if checkout date has passed
function hasCheckoutPassed(checkoutDate) {
  if (!checkoutDate || checkoutDate.toString().trim() === '') {
    return false; // No checkout date means not checked out
  }
  
  try {
    // Try to parse the date - handle various formats
    let dateObj;
    const dateStr = checkoutDate.toString().trim();
    
    // Try DD.MM.YYYY format (European format from your data)
    if (dateStr.match(/^\d{1,2}\.\d{1,2}\.\d{4}$/)) {
      const parts = dateStr.split('.');
      // DD.MM.YYYY -> YYYY-MM-DD for Date parsing
      dateObj = new Date(`${parts[2]}-${parts[1].padStart(2, '0')}-${parts[0].padStart(2, '0')}`);
    }
    // Try YYYY-MM-DD format
    else if (dateStr.match(/^\d{4}-\d{1,2}-\d{1,2}$/)) {
      dateObj = new Date(dateStr);
    }
    // Try MM/DD/YYYY format
    else if (dateStr.match(/^\d{1,2}\/\d{1,2}\/\d{4}$/)) {
      dateObj = new Date(dateStr);
    }
    // Try any other format that Date can parse
    else {
      dateObj = new Date(dateStr);
    }
    
    // If we couldn't parse the date, treat as not checked out
    if (isNaN(dateObj.getTime())) {
      console.warn('Failed to parse checkout date:', checkoutDate);
      return false;
    }
    
    const today = new Date();
    
    // Reset times to compare just dates (set both to midnight)
    const checkoutDateOnly = new Date(dateObj.getFullYear(), dateObj.getMonth(), dateObj.getDate());
    const todayDateOnly = new Date(today.getFullYear(), today.getMonth(), today.getDate());
    
    return checkoutDateOnly < todayDateOnly;
  } catch (e) {
    // If we can't parse the date, treat as not checked out
    console.warn('Failed to parse checkout date:', checkoutDate, e);
    return false;
  }
}

function readGuests(){
  if(!fs.existsSync(CSV_PATH)) return []
  try{
    const txt = fs.readFileSync(CSV_PATH, 'utf8')
    const records = parse(txt, { columns: true, skip_empty_lines: true })
    return records.map(r=>{
      const firstName = r['First Name'] || r['Firstname'] || r['Given Name'] || r['Name'] || r['Guest Name'] || r['Guest'] || ''
      const lastName = r['Last Name'] || r['Lastname'] || r['Surname'] || r['Family Name'] || ''
      const roomNumber = r['Room Number'] || r['Room'] || r['room'] || r['RoomNumber'] || ''
      const phoneRaw = r['Ph.'] || r['Phone'] || r['Telephone'] || r['Mobile'] || r['Contact'] || ''
      const phone = phoneRaw ? phoneRaw.toString().replace(/\s+/g,'') : ''
      const email = r['Email'] || r['E-mail'] || r['Email Address'] || ''
      const checkoutDate = r['Checkout Date'] || r['Check Out'] || r['Departure Date'] || r['Departure'] || r['CheckOut'] || ''
      const availableIn = r['Available In'] || r['Available in'] || r['Available'] || ''
      return { firstName, lastName, roomNumber, identifierValue: phone, email, checkoutDate, availableIn }
    })
  }catch(e){
    console.error('readGuests parse error', e)
    return []
  }
}

// Lightweight normalizer for Bird message objects used by SSE and guest inference
function normalizeMessage(m){
  if(!m) return null
  try{
    const body = (m.body && m.body.text && m.body.text.text) || (m.body && m.body.list && m.body.list.text) || (m.body && m.body.image && m.body.image.text) || ''
    return {
      id: m.id,
      direction: m.direction || (m.sender && m.sender.connector ? 'outgoing' : 'incoming'),
      createdAt: m.createdAt,
      body,
      raw: m
    }
  }catch(e){
    return { id: m && m.id, raw: m }
  }
}
// Look for `instay_output.csv` in the server folder first, then the project root
const CSV_PATH = (() => {
  const p1 = path.join(process.cwd(),'instay_output.csv')
  const p2 = path.join(process.cwd(),'..','instay_output.csv')
  if(fs.existsSync(p1)) return p1
  if(fs.existsSync(p2)) return p2
  return p1
})()
const BIRD_API_KEY = process.env.BIRD_API_KEY
const WORKSPACE = process.env.BIRD_WORKSPACE_ID
const CHANNEL = process.env.BIRD_CHANNEL_ID

// Helper to fetch recent messages from Bird API. Returns an array of message objects.
async function fetchBirdMessages(){
  try{
    if(!BIRD_API_KEY || !WORKSPACE || !CHANNEL) return []
    const url = `https://api.bird.com/workspaces/${WORKSPACE}/channels/${CHANNEL}/messages?limit=500`
    const r = await fetch(url, { headers: { Authorization: `AccessKey ${BIRD_API_KEY}`, Accept: 'application/json' } })
    const j = await r.json()
    return j && j.results ? j.results : []
  }catch(e){
    console.warn('fetchBirdMessages error', e)
    return []
  }
}

// persistent phone->name mapping stored on disk so names survive CSV refreshes
const PHONE_MAP_PATH = path.join(process.cwd(), '..', 'phone_name_map.json') // store at project root
let persistentPhoneMap = {}
try{
  if(fs.existsSync(PHONE_MAP_PATH)){
    const txt = fs.readFileSync(PHONE_MAP_PATH, 'utf8')
    persistentPhoneMap = JSON.parse(txt || '{}')
  }
}catch(e){ console.warn('failed to load persistent phone map', e); persistentPhoneMap = {} }

function savePersistentPhoneMap(){
  try{
    fs.writeFileSync(PHONE_MAP_PATH, JSON.stringify(persistentPhoneMap, null, 2), 'utf8')
    // Also attempt to upload to Azure blob storage for central persistence
    try{
      if(azureAvailable && AZURE_CONN){
        (async ()=>{
          try{
            const serviceClient = BlobServiceClient.fromConnectionString(AZURE_CONN)
            const containerClient = serviceClient.getContainerClient('instay-maps')
            await containerClient.createIfNotExists()
            const blobClient = containerClient.getBlockBlobClient('phone_name_map.json')
            const content = Buffer.from(JSON.stringify(persistentPhoneMap, null, 2), 'utf8')
            await blobClient.uploadData(content, { blobHTTPHeaders: { blobContentType: 'application/json' } })
            console.log('[azure] uploaded phone_name_map.json to instay-maps')
          }catch(e){ console.warn('azure upload phone map failed', e) }
        })()
      }
    }catch(e){ /* ignore azure errors */ }
  }catch(e){ console.error('failed to save phone map', e) }
}

// Persistent blocklist for phones that have already received a template send
const SENT_TEMPLATE_BLOCKLIST_PATH = path.join(process.cwd(), '..', 'sent_template_blocklist.json')
let sentTemplateBlocklist = new Set()
try{
  if(fs.existsSync(SENT_TEMPLATE_BLOCKLIST_PATH)){
    const txt = fs.readFileSync(SENT_TEMPLATE_BLOCKLIST_PATH, 'utf8')
    const arr = JSON.parse(txt || '[]')
    if(Array.isArray(arr)) arr.forEach(p=>{ if(p) sentTemplateBlocklist.add(String(p)) })
  }
}catch(e){ console.warn('failed to load sent template blocklist', e); sentTemplateBlocklist = new Set() }

function saveSentTemplateBlocklist(){
  try{
    const arr = Array.from(sentTemplateBlocklist.values())
    fs.writeFileSync(SENT_TEMPLATE_BLOCKLIST_PATH, JSON.stringify(arr, null, 2), 'utf8')
    // attempt upload to Azure for central persistence (best-effort)
    try{
      if(azureAvailable && AZURE_CONN){
        (async ()=>{
          try{
            const serviceClient = BlobServiceClient.fromConnectionString(AZURE_CONN)
            const containerClient = serviceClient.getContainerClient('instay-maps')
            await containerClient.createIfNotExists()
            const blobClient = containerClient.getBlockBlobClient('sent_template_blocklist.json')
            const content = Buffer.from(JSON.stringify(arr, null, 2), 'utf8')
            await blobClient.uploadData(content, { blobHTTPHeaders: { blobContentType: 'application/json' } })
            console.log('[azure] uploaded sent_template_blocklist.json to instay-maps')
          }catch(e){ console.warn('azure upload sent blocklist failed', e) }
        })()
      }
    }catch(e){ /* ignore azure errors */ }
  }catch(e){ console.error('failed to save sent template blocklist', e) }
}

// Read CSV and update persistentPhoneMap with phone->name entries
function updatePhoneMapFromCsv(){
  try{
    if(!fs.existsSync(CSV_PATH)) return
    const txt = fs.readFileSync(CSV_PATH, 'utf8')
    const records = parse(txt, { columns: true, skip_empty_lines: true })
    let changed = false
    records.forEach(r=>{
      try{
        const firstName = r['First Name'] || r['Firstname'] || r['Given Name'] || r['Name'] || r['Guest Name'] || r['Guest'] || ''
        const lastName = r['Last Name'] || r['Lastname'] || r['Surname'] || r['Family Name'] || ''
        const phoneRaw = r['Ph.'] || r['Phone'] || r['Telephone'] || r['Mobile'] || r['Contact'] || ''
        const phone = phoneRaw ? phoneRaw.toString().replace(/\s+/g,'') : ''
        const norm = (phone||'').toString().replace(/\D/g,'')
        const fullname = ((firstName||'').toString().trim() + (lastName ? (' ' + (lastName||'').toString().trim()) : '')).trim()
        if(norm && fullname){
          // attempt to capture checkout date from CSV row
          const checkoutRaw = r['Checkout Date'] || r['Check Out'] || r['Departure Date'] || r['Departure'] || r['CheckOut'] || ''
          const checkout = checkoutRaw ? String(checkoutRaw).trim() : ''
          const existing = persistentPhoneMap[norm]
          // support existing string-based map entries and object-based entries
          const existingName = existing && typeof existing === 'object' ? existing.name : existing
          const existingCheckout = existing && typeof existing === 'object' ? existing.checkoutDate : ''
          if(!existingName){
            // store as object with name and checkoutDate (if present)
            persistentPhoneMap[norm] = checkout ? { name: fullname, checkoutDate: checkout } : fullname
            changed = true
            console.log(`[phone-map] imported from CSV: ${norm} -> ${fullname}`)
          } else {
            // if we have a mapping but no checkoutDate, and CSV provides one, update object
            if(checkout && !existingCheckout){
              persistentPhoneMap[norm] = (typeof existing === 'object') ? Object.assign({}, existing, { checkoutDate: checkout }) : { name: existingName, checkoutDate: checkout }
              changed = true
              console.log(`[phone-map] updated checkout for ${norm} -> ${checkout}`)
            } else {
              console.log(`[phone-map] skipped existing mapping for ${norm} (kept: ${existingName || existing})`)
            }
          }
        }
      }catch(e){}
    })
    if(changed) savePersistentPhoneMap()
  }catch(e){ console.error('updatePhoneMapFromCsv error', e) }
}

// Update persistentPhoneMap from an array of normalized records (as returned by readGuests() or getCsvRecordsFromAzure())
function updatePhoneMapFromRecords(records){
  try{
    if(!records || !records.length) return
    let changed = false
    records.forEach(r=>{
      try{
        const firstName = r.firstName || r['First Name'] || ''
        const lastName = r.lastName || r['Last Name'] || ''
        const phoneRaw = r.identifierValue || r['Ph.'] || r['Phone'] || r['Telephone'] || r['Mobile'] || r['Contact'] || ''
        const phone = phoneRaw ? phoneRaw.toString().replace(/\s+/g,'') : ''
        const norm = (phone||'').toString().replace(/\D/g,'')
        const fullname = ((firstName||'').toString().trim() + (lastName ? (' ' + (lastName||'').toString().trim()) : '')).trim()
        const checkoutRaw = r.checkoutDate || r['Checkout Date'] || ''
        const checkout = checkoutRaw ? String(checkoutRaw).trim() : ''
        if(norm && fullname){
          const existing = persistentPhoneMap[norm]
          const existingName = existing && typeof existing === 'object' ? existing.name : existing
          const existingCheckout = existing && typeof existing === 'object' ? existing.checkoutDate : ''
          if(!existingName){
            persistentPhoneMap[norm] = checkout ? { name: fullname, checkoutDate: checkout } : fullname
            changed = true
            console.log(`[phone-map] imported from records: ${norm} -> ${fullname}`)
          } else if(checkout && !existingCheckout){
            persistentPhoneMap[norm] = (typeof existing === 'object') ? Object.assign({}, existing, { checkoutDate: checkout }) : { name: existingName, checkoutDate: checkout }
            changed = true
            console.log(`[phone-map] updated checkout for ${norm} -> ${checkout}`)
          }
        }
      }catch(e){}
    })
    if(changed) savePersistentPhoneMap()
  }catch(e){ console.error('updatePhoneMapFromRecords error', e) }
}

// Azure Blob Storage setup for archiving daily CSVs
// IMPORTANT: do NOT include secrets in source. Provide connection string via
// the environment variable `AZURE_STORAGE_CONNECTION_STRING`. The hardcoded
// connection string that used to be here was removed to avoid leaking secrets
// into git history.
let AZURE_CONN = process.env.AZURE_STORAGE_CONNECTION_STRING || ''
let azureAvailable = false
let { BlobServiceClient } = {}
try{
  const azure = require('@azure/storage-blob')
  BlobServiceClient = azure.BlobServiceClient
  azureAvailable = true
}catch(e){
  console.warn('Azure Storage SDK not installed or failed to load; CSV archiving disabled')
}

// Path for a generated phone->name index aggregated across all archived CSVs
// Stored in project-root `instay_archive/instay_archives_phone_index.json`
const PHONE_INDEX_DIR = path.join(process.cwd(), '..', 'instay_archive')
if(!fs.existsSync(PHONE_INDEX_DIR)) try{ fs.mkdirSync(PHONE_INDEX_DIR, { recursive: true }) }catch(e){}
const PHONE_INDEX_PATH = path.join(PHONE_INDEX_DIR, 'instay_archives_phone_index.json')

// Build an aggregated phone->name index from all CSV files in the Azure
// `instay-archives` container. The result maps normalized phone -> info:
// { name, occurrences: [ { file, phone, checkoutDate, firstName, lastName, roomNumber } ], latestCheckout }
async function updatePhoneIndexFromAzureArchives(){
  try{
    if(!azureAvailable || !AZURE_CONN) return null
    const serviceClient = BlobServiceClient.fromConnectionString(AZURE_CONN)
    const containerClient = serviceClient.getContainerClient('instay-archives')
    const blobs = []
    for await (const b of containerClient.listBlobsFlat()) blobs.push(b.name)
    if(!blobs.length) return null

    const index = {}
    // iterate blobs (could be optimized with concurrency for many files)
    for(const name of blobs){
      try{
        const blobClient = containerClient.getBlobClient(name)
        const dl = await blobClient.download()
        const chunks = []
        for await (const chunk of dl.readableStreamBody) chunks.push(chunk)
        const buf = Buffer.concat(chunks)
        const txt = buf.toString('utf8')
        if(!txt) continue
        const raw = parse(txt, { columns: true, skip_empty_lines: true })
        raw.forEach(r=>{
          try{
            const firstName = (r['First Name'] || r['Firstname'] || r['Given Name'] || r['Name'] || r['Guest Name'] || r['Guest'] || '').toString().trim()
            const lastName = (r['Last Name'] || r['Lastname'] || r['Surname'] || r['Family Name'] || '').toString().trim()
            const phoneRaw = (r['Ph.'] || r['Phone'] || r['Telephone'] || r['Mobile'] || r['Contact'] || '').toString().replace(/\s+/g,'')
            const norm = (phoneRaw||'').replace(/\D/g,'')
            const checkout = (r['Checkout Date'] || r['Check Out'] || r['Departure Date'] || r['Departure'] || r['CheckOut'] || '').toString().trim()
            const roomNumber = (r['Room Number'] || r['Room'] || r['room'] || r['RoomNumber'] || '').toString().trim()
            const fullname = ((firstName||'') + (lastName ? (' ' + lastName) : '')).trim()
            if(!norm) return
            index[norm] = index[norm] || { name: fullname || null, occurrences: [], latestCheckout: null, _seen: new Set() }
            // dedupe occurrence by signature
            const sig = [name, phoneRaw || '', checkout || '', firstName || '', lastName || '', roomNumber || ''].join('|')
            if(!index[norm]._seen.has(sig)){
              index[norm]._seen.add(sig)
              index[norm].occurrences.push({ file: name, phone: phoneRaw, checkoutDate: checkout || null, firstName: firstName || null, lastName: lastName || null, roomNumber: roomNumber || null })
            }
            if(fullname && !index[norm].name) index[norm].name = fullname
            // track latest checkout date (ISO-parseable wins)
            if(checkout){
              const d = new Date(checkout)
              if(!isNaN(d)){
                if(!index[norm].latestCheckout || new Date(index[norm].latestCheckout) < d) index[norm].latestCheckout = checkout
              } else if(!index[norm].latestCheckout) index[norm].latestCheckout = checkout
            }
          }catch(e){}
        })
      }catch(e){ /* ignore single-blob failures */ }
    }

    // Merge with any existing local phone index so we never overwrite previous
    // occurrences; append new occurrences and keep latestCheckout/name where possible.
    try{
      let existing = {}
      try{ if(fs.existsSync(PHONE_INDEX_PATH)) existing = JSON.parse(fs.readFileSync(PHONE_INDEX_PATH,'utf8')||'{}') }catch(e){ existing = {} }

      const merged = Object.assign({}, existing)

      // helper to create a signature for an occurrence for deduping
      const occSig = (o, fileName) => {
        try{
          return [fileName || (o && o.file) || '', (o && o.phone) || '', (o && o.checkoutDate) || '', (o && o.firstName) || '', (o && o.lastName) || '', (o && o.roomNumber) || ''].join('|')
        }catch(e){ return JSON.stringify(o||'') }
      }

      Object.keys(index).forEach(k => {
        const src = index[k]
        const dst = merged[k] || { name: null, occurrences: [], latestCheckout: null }

        // build seen set from existing occurrences
        const seen = new Set()
        try{ (dst.occurrences||[]).forEach(o=> seen.add(occSig(o, o && o.file))) }catch(e){}

        // append new occurrences from src, deduping by signature
        (src.occurrences||[]).forEach(o=>{
          const sig = occSig(o, src && src.file)
          if(!seen.has(sig)){
            // ensure file property is present on occurrence
            const copy = Object.assign({}, o)
            if(!copy.file && src && src.file) copy.file = src.file
            dst.occurrences = dst.occurrences || []
            dst.occurrences.push(copy)
            seen.add(sig)
          }
        })

        // preserve existing name when present, otherwise take from src
        if(!dst.name && src.name) dst.name = src.name
        // reconcile latestCheckout (prefer parseable ISO/latest date)
        const candDates = []
        if(dst.latestCheckout) candDates.push(dst.latestCheckout)
        if(src.latestCheckout) candDates.push(src.latestCheckout)
        // also include checkoutDates from occurrences
        (dst.occurrences||[]).forEach(o=>{ if(o && o.checkoutDate) candDates.push(o.checkoutDate) })
        try{
          let best = null
          candDates.forEach(cd=>{
            try{
              const d = new Date(cd)
              if(!isNaN(d)){
                if(!best || new Date(best) < d) best = cd
              } else if(!best) best = cd
            }catch(e){}
          })
          dst.latestCheckout = best || null
        }catch(e){}

        merged[k] = dst
      })

      // ensure we remove any transient/internal fields and normalize structure
      const out = {}
      Object.keys(merged).forEach(k => {
        const v = merged[k]
        out[k] = { name: v.name || null, occurrences: v.occurrences || [], latestCheckout: v.latestCheckout || null }
      })

      fs.writeFileSync(PHONE_INDEX_PATH, JSON.stringify(out, null, 2), 'utf8')
    }catch(e){ console.warn('failed to write local phone index', e) }

    // upload to instay-maps container for central consumption
    try{
      // upload the merged local index (if present) so we don't clobber prior data
      let toUpload = null
      try{ toUpload = JSON.parse(fs.readFileSync(PHONE_INDEX_PATH,'utf8')||'{}') }catch(e){ toUpload = index }
      const mapsClient = serviceClient.getContainerClient('instay-maps')
      await mapsClient.createIfNotExists()
      const blobClient = mapsClient.getBlockBlobClient('instay_archives_phone_index.json')
      const content = Buffer.from(JSON.stringify(toUpload, null, 2), 'utf8')
      await blobClient.uploadData(content, { blobHTTPHeaders: { blobContentType: 'application/json' } })
      console.log('[azure] uploaded instay_archives_phone_index.json to instay-maps')
    }catch(e){ console.warn('failed to upload phone index to azure', e) }

    return index
  }catch(e){ console.error('updatePhoneIndexFromAzureArchives error', e); return null }
}

// Build phone index from local CSV files found in common locations.
async function updatePhoneIndexFromLocalArchives(){
  try{
    const candidates = [PHONE_INDEX_DIR, process.cwd(), path.join(process.cwd(),'server'), path.join(process.cwd(),'..')]
    const csvFiles = []
    candidates.forEach(dir => {
      try{
        if(!fs.existsSync(dir)) return
        const items = fs.readdirSync(dir)
        items.forEach(f => {
          try{
            if(!f) return
            const lower = f.toString().toLowerCase()
            if(lower.endsWith('.csv') && (lower.includes('instay') || lower.includes('output') || lower.includes('guest') || lower.includes('instay_output'))){
              csvFiles.push(path.join(dir, f))
            }
          }catch(e){}
        })
      }catch(e){}
    })
    if(!csvFiles.length) return null

    const index = {}
    csvFiles.forEach(filePath => {
      try{
        const txt = fs.readFileSync(filePath, 'utf8')
        if(!txt) return
        const raw = parse(txt, { columns: true, skip_empty_lines: true })
        raw.forEach(r => {
          try{
            const firstName = (r['First Name'] || r['Firstname'] || r['Given Name'] || r['Name'] || r['Guest Name'] || r['Guest'] || '').toString().trim()
            const lastName = (r['Last Name'] || r['Lastname'] || r['Surname'] || r['Family Name'] || '').toString().trim()
            const phoneRaw = (r['Ph.'] || r['Phone'] || r['Telephone'] || r['Mobile'] || r['Contact'] || '').toString().replace(/\s+/g,'')
            const norm = (phoneRaw||'').replace(/\D/g,'')
            const checkout = (r['Checkout Date'] || r['Check Out'] || r['Departure Date'] || r['Departure'] || r['CheckOut'] || '').toString().trim()
            const roomNumber = (r['Room Number'] || r['Room'] || r['room'] || r['RoomNumber'] || '').toString().trim()
            const fullname = ((firstName||'') + (lastName ? (' ' + lastName) : '')).trim()
            if(!norm) return
            index[norm] = index[norm] || { name: fullname || null, occurrences: [], latestCheckout: null, _seen: new Set() }
            const sig = [filePath, phoneRaw || '', checkout || '', firstName || '', lastName || '', roomNumber || ''].join('|')
            if(!index[norm]._seen.has(sig)){
              index[norm]._seen.add(sig)
              index[norm].occurrences.push({ file: path.basename(filePath), phone: phoneRaw, checkoutDate: checkout || null, firstName: firstName || null, lastName: lastName || null, roomNumber: roomNumber || null })
            }
            if(fullname && !index[norm].name) index[norm].name = fullname
            if(checkout){
              const d = new Date(checkout)
              if(!isNaN(d)){
                if(!index[norm].latestCheckout || new Date(index[norm].latestCheckout) < d) index[norm].latestCheckout = checkout
              } else if(!index[norm].latestCheckout) index[norm].latestCheckout = checkout
            }
          }catch(e){}
        })
      }catch(e){}
    })

    // Merge with existing local phone index to preserve prior occurrences
    try{
      let existing = {}
      try{ if(fs.existsSync(PHONE_INDEX_PATH)) existing = JSON.parse(fs.readFileSync(PHONE_INDEX_PATH,'utf8')||'{}') }catch(e){ existing = {} }
      const merged = Object.assign({}, existing)
      const occSig = (o, fileName) => {
        try{ return [fileName || (o && o.file) || '', (o && o.phone) || '', (o && o.checkoutDate) || '', (o && o.firstName) || '', (o && o.lastName) || '', (o && o.roomNumber) || ''].join('|') }catch(e){ return JSON.stringify(o||'') }
      }
      Object.keys(index).forEach(k => {
        const src = index[k]
        const dst = merged[k] || { name: null, occurrences: [], latestCheckout: null }
        const seen = new Set()
        try{ (dst.occurrences||[]).forEach(o=> seen.add(occSig(o, o && o.file))) }catch(e){}
        (src.occurrences||[]).forEach(o=>{
          const sig = occSig(o, src && src.file)
          if(!seen.has(sig)){
            const copy = Object.assign({}, o)
            if(!copy.file && src && src.file) copy.file = src.file
            dst.occurrences = dst.occurrences || []
            dst.occurrences.push(copy)
            seen.add(sig)
          }
        })
        if(!dst.name && src.name) dst.name = src.name
        // reconcile latestCheckout
        const candDates = []
        if(dst.latestCheckout) candDates.push(dst.latestCheckout)
        if(src.latestCheckout) candDates.push(src.latestCheckout)
        (dst.occurrences||[]).forEach(o=>{ if(o && o.checkoutDate) candDates.push(o.checkoutDate) })
        try{
          let best = null
          candDates.forEach(cd=>{
            try{
              const d = new Date(cd)
              if(!isNaN(d)){
                if(!best || new Date(best) < d) best = cd
              } else if(!best) best = cd
            }catch(e){}
          })
          dst.latestCheckout = best || null
        }catch(e){}
        merged[k] = dst
      })

      const out = {}
      Object.keys(merged).forEach(k => {
        const v = merged[k]
        out[k] = { name: v.name || null, occurrences: v.occurrences || [], latestCheckout: v.latestCheckout || null }
      })
      fs.writeFileSync(PHONE_INDEX_PATH, JSON.stringify(out, null, 2), 'utf8')
    }catch(e){ console.warn('failed to write local phone index (local)', e) }

    return index
  }catch(e){ console.error('updatePhoneIndexFromLocalArchives error', e); return null }
}


// If Azure is available, try to load an existing phone_name_map.json from blob storage
if(azureAvailable && AZURE_CONN){
  (async ()=>{
    try{
      const serviceClient = BlobServiceClient.fromConnectionString(AZURE_CONN)
      const containerClient = serviceClient.getContainerClient('instay-maps')
      const blobClient = containerClient.getBlobClient('phone_name_map.json')
      try{
        const dl = await blobClient.download()
        const chunks = []
        for await (const chunk of dl.readableStreamBody) chunks.push(chunk)
        const buf = Buffer.concat(chunks)
        const txt = buf.toString('utf8')
        const remote = JSON.parse(txt || '{}')
        // merge remote into local persistent map (favor local values when present)
        Object.keys(remote || {}).forEach(k => {
          if(!persistentPhoneMap[k]) persistentPhoneMap[k] = remote[k]
        })
        // persist merged map locally
        try{ fs.writeFileSync(PHONE_MAP_PATH, JSON.stringify(persistentPhoneMap, null, 2), 'utf8') }catch(e){}
        console.log('[azure] loaded phone_name_map.json from instay-maps')
      }catch(e){ /* blob missing or failed */ }
    }catch(e){ console.warn('failed to load phone map from azure', e) }
    // Also attempt to build/load the aggregated phone index from archives
    try{ await updatePhoneIndexFromAzureArchives() }catch(e){ /* ignore */ }
  })()
}

  // Always attempt to build a local phone index from any CSVs present locally
  (async ()=>{
    try{ await updatePhoneIndexFromLocalArchives() }catch(e){}
  })()

  // Poll Azure for the latest `instay_output_latest.csv` and ingest when it changes
  let lastAzureLatestMtime = null
  async function pollAzureLatestCsv(){
    try{
      if(!azureAvailable || !AZURE_CONN) return
      const serviceClient = BlobServiceClient.fromConnectionString(AZURE_CONN)
      const containerClient = serviceClient.getContainerClient('instay-archives')
      const blobClient = containerClient.getBlobClient('instay_output_latest.csv')
      try{
        const props = await blobClient.getProperties()
        const mtime = props.lastModified && new Date(props.lastModified).toISOString()
        if(mtime && mtime !== lastAzureLatestMtime){
          lastAzureLatestMtime = mtime
          // download blob
          const dl = await blobClient.download()
          const chunks = []
          for await (const chunk of dl.readableStreamBody) chunks.push(chunk)
          const buf = Buffer.concat(chunks)
          // write to local CSV path
          try{ fs.writeFileSync(CSV_PATH, buf) }catch(e){ console.warn('failed to write latest CSV locally', e) }
          console.log('[azure] detected updated instay_output_latest.csv — ingesting to phone map')
          // ingest into persistent map and update indexes
          try{ updatePhoneMapFromCsv() }catch(e){ console.error('phone-map update error (azure poll)', e) }
          try{ await updatePhoneIndexFromAzureArchives() }catch(e){ console.error('phone-index update error (azure poll)', e) }
          try{ await updatePhoneIndexFromLocalArchives() }catch(e){ console.error('phone-index local update error (azure poll)', e) }
        }
      }catch(e){ /* blob missing or access error */ }
    }catch(e){ console.warn('pollAzureLatestCsv error', e) }
  }

  // start poller when Azure is available
  try{ if(azureAvailable && AZURE_CONN){ pollAzureLatestCsv().catch(()=>{}); setInterval(()=>{ pollAzureLatestCsv().catch(()=>{}) }, 30 * 1000) } }catch(e){}

async function uploadCsvArchiveIfNeeded(dateStr){
  try{
    if(!fs.existsSync(CSV_PATH)) return
    if(!azureAvailable) return
    if(!AZURE_CONN) return

    const containerName = 'instay-archives'
    const serviceClient = BlobServiceClient.fromConnectionString(AZURE_CONN)
    const containerClient = serviceClient.getContainerClient(containerName)
    try{ await containerClient.createIfNotExists() }catch(e){}

    // Use precise timestamped filename so every upload is preserved.
    const now = new Date()
    const datePart = now.toISOString().slice(0,10)
    const timePart = now.toISOString().replace(/[:\.]/g,'')
    const filename = `instay_output-${datePart}-${timePart}.csv`

    const blockBlobClient = containerClient.getBlockBlobClient(filename)
    const content = fs.readFileSync(CSV_PATH)
    await blockBlobClient.uploadData(content, { blobHTTPHeaders: { blobContentType: 'text/csv' } })
    console.log(`[azure] uploaded CSV archive to ${containerName}/${filename}`)
  }catch(e){ console.error('azure upload error', e) }
}

// Try to fetch the latest instay CSV from Azure archive container and return parsed records
async function getCsvRecordsFromAzure(){
  try{
    if(!azureAvailable || !AZURE_CONN) return null
    const serviceClient = BlobServiceClient.fromConnectionString(AZURE_CONN)
    const containerClient = serviceClient.getContainerClient('instay-archives')
    // collect blobs and find the most recent
    const items = []
    for await (const blob of containerClient.listBlobsFlat()){
      items.push({ name: blob.name, lastModified: blob.properties && blob.properties.lastModified })
    }
    if(!items.length) return null
    items.sort((a,b)=> new Date(b.lastModified) - new Date(a.lastModified))
    const latest = items[0]
    const blobClient = containerClient.getBlobClient(latest.name)
    const dl = await blobClient.download()
    const chunks = []
    for await (const chunk of dl.readableStreamBody) chunks.push(chunk)
    const buf = Buffer.concat(chunks)
    const txt = buf.toString('utf8')
    if(!txt) return null
    try{
      const raw = parse(txt, { columns: true, skip_empty_lines: true })
      // normalize rows to match readGuests() output shape
      const records = raw.map(r=>{
        const firstName = r['First Name'] || r['Firstname'] || r['Given Name'] || r['Name'] || r['Guest Name'] || r['Guest'] || ''
        const lastName = r['Last Name'] || r['Lastname'] || r['Surname'] || r['Family Name'] || ''
        const roomNumber = r['Room Number'] || r['Room'] || r['room'] || r['RoomNumber'] || ''
        const phoneRaw = r['Ph.'] || r['Phone'] || r['Telephone'] || r['Mobile'] || r['Contact'] || ''
        const phone = phoneRaw ? phoneRaw.toString().replace(/\s+/g,'') : ''
        const email = r['Email'] || r['E-mail'] || r['Email Address'] || ''
        const checkoutDate = r['Checkout Date'] || r['Check Out'] || r['Departure Date'] || r['Departure'] || r['CheckOut'] || ''
        const availableIn = r['Available In'] || r['Available in'] || r['Available'] || ''
        return { firstName, lastName, roomNumber, identifierValue: phone, email, checkoutDate, availableIn }
      })
      return records
    }catch(e){
      console.warn('failed to parse azure CSV', e)
      return null
    }
  }catch(e){
    // ignore errors and fall back to local CSV
    return null
  }
}

// Watch CSV file mtime and upload when it changes (polling for robustness)
let lastCsvMtimeObserved = null
function checkCsvAndUpload(){
  try{
    if(!fs.existsSync(CSV_PATH)) return
    const st = fs.statSync(CSV_PATH)
    const mdateFull = new Date(st.mtime).toISOString()
    if(mdateFull !== lastCsvMtimeObserved){
      console.log(`[azure] CSV changed; detected mtime ${mdateFull}, uploading archive`)
      lastCsvMtimeObserved = mdateFull
      uploadCsvArchiveIfNeeded(mdateFull)
        .catch(e=>console.error('upload error', e))
          .then(async ()=>{
            // After upload (or attempted upload), ingest names from the CSV into persistent map
            try{ updatePhoneMapFromCsv() }catch(e){ console.error('phone-map update error', e) }
            // Also update aggregated phone index across all archived CSVs and upload it
            try{ await updatePhoneIndexFromAzureArchives() }catch(e){ console.error('phone-index update error', e) }
            // ensure local fallback index is refreshed as well
            try{ await updatePhoneIndexFromLocalArchives() }catch(e){ console.error('phone-index local update error', e) }
          })
    }
  }catch(e){ console.error('csv watch error', e) }
}

// run initial check and poll every 30s
try{ checkCsvAndUpload(); setInterval(checkCsvAndUpload, 30 * 1000) }catch(e){/* ignore */}

// Try to extract a guest name from a Bird message's template variables or body text
function extractNameFromMessage(m){
  try{
    // check template variables (case-insensitive keys) for name-like values
    const vars = (m.template && m.template.variables) || {}
    for(const [k,v] of Object.entries(vars || {})){
      try{
        const key = String(k||'').toLowerCase()
        const sval = (v||'').toString().trim()
        if(!sval) continue
        // prefer keys that look like first/last/name
        if(key.includes('first') || key.includes('name') || key.includes('guest')) return sval
      }catch(e){}
    }
    // as a fallback, scan all variables for a short alpha-only value
    for(const v of Object.values(vars)){
      const s = String(v||'').trim()
      if(s && /^[A-Za-z\-\' ]{2,60}$/.test(s)) return s
    }
    // try to parse a leading name from text body like "Hi Robert," or "Dear Emma"
    const body = (m.body && m.body.text && m.body.text.text) || (m.body && m.body.list && m.body.list.text) || (m.body && m.body.image && m.body.image.text) || ''
    if(body){
      // match greetings like "Dear John," or "Dear John Smith," (capture 1-2 words)
      const match = body.trim().match(/^(?:hi|hello|dear|hey)\s+["']?([A-Za-z\-\']+(?:\s+[A-Za-z\-\']{1,40})?)["']?[,\.!\s]/i)
      if(match && match[1]) return match[1].trim()
      // also check for a single leading word followed by comma (e.g., "John, Please...")
      const m2 = body.trim().match(/^["']?([A-Za-z\-\']{2,40})["']?[,\s]/)
      if(m2 && m2[1]) return m2[1].trim()
      // as a last resort, look for "Dear: Name" patterns
      const m3 = body.trim().match(/Dear[:\s]+["']?([A-Za-z\-\']+(?:\s+[A-Za-z\-\']{1,40})?)["']?/i)
      if(m3 && m3[1]) return m3[1].trim()
    }
  }catch(e){}
  return null
}

// --- Server-Sent Events (SSE) for live updates ---
const sseClients = new Set()
const seenMessageIds = new Set()

app.get('/api/stream', (req,res)=>{
  res.setHeader('Content-Type','text/event-stream')
  res.setHeader('Cache-Control','no-cache')
  res.setHeader('Connection','keep-alive')
  res.flushHeaders && res.flushHeaders()
  res.write(': connected\n\n')
  sseClients.add(res)
  req.on('close', ()=>{ sseClients.delete(res) })
})

function sendSSE(event, data){
  const payload = typeof data === 'string' ? data : JSON.stringify(data)
  for(const res of sseClients){
    try{ res.write(`event: ${event}\ndata: ${payload}\n\n`) }catch(e){ /* ignore */ }
  }
}

// poll Bird for new messages and push via SSE
;(async ()=>{
  try{
    const initial = await fetchBirdMessages()
    initial.forEach(m=> m.id && seenMessageIds.add(m.id))
  }catch(e){/* ignore */}
  setInterval(async ()=>{
    try{
      const msgs = await fetchBirdMessages()
      // sort ascending so we push oldest-first
      msgs.sort((a,b)=> new Date(a.createdAt) - new Date(b.createdAt))
      for(const m of msgs){
        if(m && m.id && !seenMessageIds.has(m.id)){
          seenMessageIds.add(m.id)
          const norm = normalizeMessage(m)
          sendSSE('message', norm)
        }
      }
    }catch(e){/* ignore polling errors */}
  }, 3000)
})()

app.get('/api/guests', (req,res)=>{
  (async ()=>{
    // Prefer the latest CSV stored in Azure archives when available (serves as source of truth)
    let csvGuests = null
    try{ csvGuests = await getCsvRecordsFromAzure() }catch(e){ csvGuests = null }
    if(!csvGuests) csvGuests = readGuests()
    // Ensure phone map is updated from Azure/local CSV records so names/checkoutDates
    // are applied when building the guest list in the sidebar.
    try{ updatePhoneMapFromRecords(csvGuests) }catch(e){}
    const guestsByPhone = {}
    // Build a quick lookup map from the CSV records (prefer Azure CSV) keyed by normalized phone
    const csvByPhone = {}
    csvGuests.forEach(r => {
      try{
        const phone = (r.identifierValue || r['Ph.'] || r['Phone'] || r['Mobile'] || '').toString().replace(/\s+/g,'')
        if(!phone) return
        const norm = phone.replace(/\D/g,'')
        const firstName = r.firstName || r['First Name'] || ''
        const lastName = r.lastName || r['Last Name'] || ''
        const roomNumber = r.roomNumber || r['Room Number'] || ''
        const checkoutDate = r.checkoutDate || r['Checkout Date'] || r['Check Out'] || r['Departure Date'] || ''
        const checkinDate = r.checkinDate || r['Checkin Date'] || r['Check-In'] || r['Arrival Date'] || ''
        const email = r.email || r['Email'] || ''
        const fullname = ((firstName||'').toString().trim() + (lastName ? (' ' + (lastName||'').toString().trim()) : '')).trim()
        const rec = { firstName, lastName, roomNumber, identifierValue: phone, email, checkoutDate, checkinDate, fullname }
        csvByPhone[norm] = rec
        csvByPhone[phone] = rec
        const alt = phone && phone.startsWith('+') ? phone.slice(1) : ('+' + phone)
        csvByPhone[alt] = rec
      }catch(e){}
    })

    // index CSV guests into guestsByPhone using normalized keys and attach CSV info
    csvGuests.forEach(g=>{
        try{
          if(g.identifierValue){
            const phone = g.identifierValue
            const alt = phone && phone.startsWith('+') ? phone.slice(1) : ('+' + phone)
            const norm = (phone||'').toString().replace(/\D/g,'')
            const base = Object.assign({ lastMessage: '', lastSeen: null, templateName: null, deliveryStatus: null, deliveryReason: null, lastDirection: '' }, g)
            // attach CSV-enriched fields from csvByPhone if available
            const csvRec = csvByPhone[norm] || csvByPhone[phone] || csvByPhone[alt]
            if(csvRec){
              base.firstName = base.firstName || csvRec.firstName || ''
              base.lastName = base.lastName || csvRec.lastName || ''
              base.roomNumber = base.roomNumber || csvRec.roomNumber || ''
              base.checkoutDate = base.checkoutDate || csvRec.checkoutDate || ''
              base.checkinDate = base.checkinDate || csvRec.checkinDate || ''
            }
            guestsByPhone[phone] = base
            guestsByPhone[alt] = base
            if(norm) guestsByPhone[norm] = base
          }
        }catch(e){}
    })

    // augment from Bird messages
    const msgs = await fetchBirdMessages()
    msgs.forEach(m=>{
      try{
        // check receiver contacts (index under normalized phone key when possible)
        if(m.receiver && m.receiver.contacts){
          m.receiver.contacts.forEach(c=>{
            const phone = (c.identifierValue||'').replace(/\s+/g,'')
            if(!phone) return
            const norm = (phone||'').toString().replace(/\D/g,'')
            const key = norm || phone
            if(!guestsByPhone[key]){
              guestsByPhone[key] = { firstName:'', lastName:'', roomNumber:'', identifierValue:phone, email:'', lastMessage:'', lastSeen:null }
            }
            // try to extract a friendly name and room from message/template
            if(!guestsByPhone[key].firstName){
              const nameFromAnnotation = c.annotations && c.annotations.name
              const nameFromTemplate = extractNameFromMessage(m)
              if(nameFromAnnotation) guestsByPhone[key].firstName = nameFromAnnotation
              else if(nameFromTemplate) guestsByPhone[key].firstName = nameFromTemplate
              else {
                // fall back to persistent phone->name mapping if present
                if(norm && persistentPhoneMap[norm]){
                  const parts = persistentPhoneMap[norm].split(' ').filter(Boolean)
                  guestsByPhone[key].firstName = parts.shift() || ''
                  guestsByPhone[key].lastName = parts.join(' ') || ''
                }
              }
            }
            if(!guestsByPhone[key].roomNumber){
              const roomFromTemplate = m.template && m.template.variables && (m.template.variables.room_number || m.template.variables.room)
              if(roomFromTemplate) guestsByPhone[key].roomNumber = roomFromTemplate
            }

            const body = (m.body && m.body.text && m.body.text.text) || (m.body && m.body.list && m.body.list.text) || (m.body && m.body.image && m.body.image.text) || JSON.stringify(m.body)
            if(!guestsByPhone[key].lastSeen || new Date(m.createdAt) > new Date(guestsByPhone[key].lastSeen)){
              guestsByPhone[key].lastMessage = body
              guestsByPhone[key].lastSeen = m.createdAt
              guestsByPhone[key].lastDirection = m.direction || guestsByPhone[key].lastDirection
            }
            // capture template name and delivery status if present
            if(m.template && m.template.name){
              guestsByPhone[key].templateName = m.template.name
            }
            if(m.status){
              guestsByPhone[key].deliveryStatus = m.status
            }
            if(m.failure && m.failure.description){
              guestsByPhone[key].deliveryReason = m.failure.description
            }
            // If there's a failure object but no explicit status, mark as failed
            if(!m.status && m.failure){
              guestsByPhone[key].deliveryStatus = guestsByPhone[key].deliveryStatus || 'failed'
            }
            // capture failure code if present (some platforms embed under failure.code or failure.source.code)
            if(m.failure && (m.failure.code || (m.failure.source && m.failure.source.code))){
              guestsByPhone[key].deliveryCode = m.failure.code || (m.failure.source && m.failure.source.code)
            }
          })
        }
        // check sender contact
        if(m.sender && m.sender.contact){
          const phone = (m.sender.contact.identifierValue||'').replace(/\s+/g,'')
          if(phone){
            const norm = phone.replace(/\D/g,'')
            if(!guestsByPhone[phone]){
              guestsByPhone[phone] = { firstName:'', lastName:'', roomNumber:'', identifierValue:phone, email:'', lastMessage:'', lastSeen:null }
            }
            // prefer sender annotations for name
            if(!guestsByPhone[phone].firstName){
              const sname = m.sender.contact.annotations && m.sender.contact.annotations.name
              const nameFromTemplate = extractNameFromMessage(m)
              if(sname) {
                guestsByPhone[phone].firstName = sname
              } else if(nameFromTemplate) {
                guestsByPhone[phone].firstName = nameFromTemplate
              } else {
                // fall back to persistent phone->name mapping if present
                if(norm && persistentPhoneMap[norm]){
                  const mapping = persistentPhoneMap[norm]
                  if(typeof mapping === 'string'){
                    const parts = mapping.split(' ').filter(Boolean)
                    guestsByPhone[phone].firstName = parts.shift() || ''
                    guestsByPhone[phone].lastName = parts.join(' ') || ''
                  } else if(mapping && typeof mapping === 'object'){
                    const parts = (mapping.name || '').split(' ').filter(Boolean)
                    guestsByPhone[phone].firstName = parts.shift() || ''
                    guestsByPhone[phone].lastName = parts.join(' ') || ''
                    if(mapping.checkoutDate) guestsByPhone[phone].checkoutDate = guestsByPhone[phone].checkoutDate || mapping.checkoutDate
                    if(mapping.checkinDate) guestsByPhone[phone].checkinDate = guestsByPhone[phone].checkinDate || mapping.checkinDate
                  }
                }
                // fall back to CSV-derived data if available
                const csvRec = csvByPhone[norm] || csvByPhone[phone]
                if(csvRec){
                  guestsByPhone[phone].firstName = guestsByPhone[phone].firstName || csvRec.firstName || ''
                  guestsByPhone[phone].lastName = guestsByPhone[phone].lastName || csvRec.lastName || ''
                  guestsByPhone[phone].checkoutDate = guestsByPhone[phone].checkoutDate || csvRec.checkoutDate || ''
                  guestsByPhone[phone].checkinDate = guestsByPhone[phone].checkinDate || csvRec.checkinDate || ''
                  guestsByPhone[phone].roomNumber = guestsByPhone[phone].roomNumber || csvRec.roomNumber || ''
                }
              }
            }
            const body = (m.body && m.body.text && m.body.text.text) || (m.body && m.body.list && m.body.list.text) || (m.body && m.body.image && m.body.image.text) || JSON.stringify(m.body)
            if(!guestsByPhone[phone].lastSeen || new Date(m.createdAt) > new Date(guestsByPhone[phone].lastSeen)){
              guestsByPhone[phone].lastMessage = body
              guestsByPhone[phone].lastSeen = m.createdAt
              guestsByPhone[phone].lastDirection = m.direction || guestsByPhone[phone].lastDirection
            }
            if(m.template && m.template.name){
              guestsByPhone[phone].templateName = m.template.name
            }

            // Ensure failures are represented as a deliveryStatus when present
            if(!m.status && m.failure){
              guestsByPhone[phone].deliveryStatus = guestsByPhone[phone].deliveryStatus || 'failed'
            }
            if(m.status){
              guestsByPhone[phone].deliveryStatus = m.status
            }
            if(m.failure && m.failure.description){
              guestsByPhone[phone].deliveryReason = m.failure.description
            }
            if(m.failure && (m.failure.code || (m.failure.source && m.failure.source.code))){
              guestsByPhone[phone].deliveryCode = m.failure.code || (m.failure.source && m.failure.source.code)
            }
          }
        }
      }catch(e){/* ignore malformed message */}
    })

    // dedupe entries (multiple phone-key variants may map to the same guest)
    // Before deduping, try to extract a stable name per phone from the earliest
    // Bird message (prefer template variables or contact annotations). Persist
    // the discovered mapping so names survive CSV refreshes.
    try{
      const msgsByPhone = {}
      ;(msgs||[]).forEach(m => {
        try{
          const rcv = m.receiver && m.receiver.contacts && m.receiver.contacts[0] && m.receiver.contacts[0].identifierValue
          const snd = m.sender && m.sender.contact && m.sender.contact.identifierValue
          const phoneRaw = (rcv || snd || '').replace(/\s+/g,'')
          if(!phoneRaw) return
          const norm = phoneRaw.replace(/\D/g,'')
          const key = norm || phoneRaw
          msgsByPhone[key] = msgsByPhone[key] || []
          msgsByPhone[key].push(m)
        }catch(e){}
      })
      for(const [phoneKey, arr] of Object.entries(msgsByPhone)){
        try{
          // skip if guest already has a name
          const current = guestsByPhone[phoneKey]
          if(current && (current.firstName || current.first || current.displayName)) continue
          // sort ascending and find first message with a name
          arr.sort((a,b)=> new Date(a.createdAt) - new Date(b.createdAt))
          let found = null
          for(const m of arr){
            // prefer receiver contact annotation name
            const ann = m.receiver && m.receiver.contacts && m.receiver.contacts[0] && m.receiver.contacts[0].annotations && m.receiver.contacts[0].annotations.name
            if(ann){ found = String(ann).trim(); break }
            const sann = m.sender && m.sender.contact && m.sender.contact.annotations && m.sender.contact.annotations.name
            if(sann){ found = String(sann).trim(); break }
            const tmpl = extractNameFromMessage(m)
            if(tmpl){ found = tmpl; break }
          }
          if(found){
            const parts = String(found||'').split(' ').filter(Boolean)
            const first = parts.shift() || ''
            const last = parts.join(' ') || ''
            if(!guestsByPhone[phoneKey]) guestsByPhone[phoneKey] = { firstName:'', lastName:'', roomNumber:'', identifierValue:phoneKey, email:'', lastMessage:'', lastSeen:null }
            guestsByPhone[phoneKey].firstName = guestsByPhone[phoneKey].firstName || first
            guestsByPhone[phoneKey].lastName = guestsByPhone[phoneKey].lastName || last
            // persist mapping by normalized phone
            try{
              const norm = (phoneKey||'').toString().replace(/\D/g,'')
              if(norm && !persistentPhoneMap[norm]){
                const fullname = (first + (last ? (' ' + last) : '')).trim()
                persistentPhoneMap[norm] = { name: fullname }
                savePersistentPhoneMap()
              }
            }catch(e){/* ignore save errors */}
          }
        }catch(e){}
      }
    }catch(e){/* ignore */}

    const uniq = []
    const seen = new Set()
    Object.values(guestsByPhone).forEach(g=>{
      const id = g.identifierValue
      if(!seen.has(id)){
        seen.add(id)
        uniq.push(g)
      }
    })
    // return as array sorted by lastSeen desc
    let final = uniq.sort((a,b)=>{
      if(!a.lastSeen) return 1
      if(!b.lastSeen) return -1
      return new Date(b.lastSeen) - new Date(a.lastSeen)
    })

    // remove unwanted/system senders permanently
    const excludedNames = ['LTG:AI-Maintenance', 'Mercure Hyde Park']
    final = final.filter(g => !excludedNames.includes((g.firstName||'').toString()))

    // Ensure persistent phone->name mappings are applied to any remaining guest
    // This is a final pass to cover cases where guests were indexed under
    // alternate phone variants and didn't get a name earlier.
    final.forEach(g => {
      try{
        const hasName = (g.firstName || g.first || '').toString().trim()
        if(!hasName){
          const phone = g.identifierValue || ''
          const norm = phone.toString().replace(/\D/g,'')
          if(norm && persistentPhoneMap[norm]){
            const mapping = persistentPhoneMap[norm]
            let nameStr = ''
            let checkout = null
            if(typeof mapping === 'string') nameStr = mapping
            else if(mapping && typeof mapping === 'object'){
              nameStr = mapping.name || ''
              checkout = mapping.checkoutDate || null
            }
            const parts = (nameStr || '').toString().split(' ').filter(Boolean)
            g.firstName = parts.shift() || ''
            g.lastName = parts.join(' ') || ''
            if(checkout) g.checkoutDate = checkout
            console.log(`[phone-map] applied mapping for ${phone} -> ${g.firstName} ${g.lastName}`)
          }
        }
      }catch(e){}
    })

    // Check if client wants to include checked-out guests
    const includeCheckedOut = req.query.include_checkedout === 'true' || 
                             req.query.include_checkedout === '1' ||
                             req.query.show_all === 'true'

    // remove guests who have already checked out based on CSV fields
    if (!includeCheckedOut) {
      final = final.filter(g => {
        try {
          // Debug logging to see what we're checking
          const phone = g.identifierValue || ''
          const norm = phone.toString().replace(/\D/g,'')
          
          // Check direct checkoutDate field
          if (g.checkoutDate) {
            const passed = hasCheckoutPassed(g.checkoutDate)
            if (passed) {
              console.log(`[checkout-filter] Filtered ${g.firstName} ${g.lastName} (${phone}) - checkout date: ${g.checkoutDate} has passed`)
              return false
            }
          }
          
          // Also check the old "availableIn" field
          if(g.availableIn){
            // try to extract a number of days; if numeric and <= 0, treat as checked-out
            const n = parseInt(String(g.availableIn).replace(/[^0-9\-]/g,''), 10)
            if(!isNaN(n) && n <= 0) {
              console.log(`[checkout-filter] Filtered ${g.firstName} ${g.lastName} (${phone}) - availableIn: ${g.availableIn}`)
              return false
            }
          }
          
          // Check persistent phone map for checkout date
          if(norm && persistentPhoneMap[norm]){
            const mapping = persistentPhoneMap[norm]
            let checkoutFromMap = null
            if(typeof mapping === 'object' && mapping.checkoutDate){
              checkoutFromMap = mapping.checkoutDate
            }
            if(checkoutFromMap && hasCheckoutPassed(checkoutFromMap)){
              console.log(`[checkout-filter] Filtered ${g.firstName} ${g.lastName} (${phone}) - phone map checkout: ${checkoutFromMap} has passed`)
              return false
            }
          }
          
          return true
        } catch(e) {
          console.warn('Error checking checkout status for guest:', g, e)
          return true // Keep guest if we can't determine
        }
      })
    }

    // support optional filtering: ?template=mlhp_&status=delivered
    const { template, status } = req.query || {}
    if(template){
      final = final.filter(g => g.templateName === template)
    }
    if(status){
      final = final.filter(g => g.deliveryStatus === status)
    }

          // By default hide guests with failed delivery statuses. Consumers may
          // override behavior by passing ?include_failed=true to include them, or
          // explicitly set ?exclude_failed=false to keep hidden (default true).
          let excludeFailed = true
          if(req.query){
            // allow explicit include flag to override default
            if(req.query.include_failed === '1' || String(req.query.include_failed).toLowerCase() === 'true') excludeFailed = false
            if(req.query.exclude_failed === '1' || String(req.query.exclude_failed).toLowerCase() === 'true') excludeFailed = true
          }
          if(excludeFailed){
            final = final.filter(g => {
              const ds = (g.deliveryStatus || '').toString().toLowerCase()
              return !(ds.includes('failed') || ds.includes('sending_failed') || ds.includes('delivery_failed'))
            })
          }

    // Debug: log mapping decisions for each guest to help trace why names may be missing
    try{
      let phoneIndexData = null
      try{ if(fs.existsSync(PHONE_INDEX_PATH)) phoneIndexData = JSON.parse(fs.readFileSync(PHONE_INDEX_PATH,'utf8')||'{}') }catch(e){ phoneIndexData = null }
      final.forEach(g => {
        try{
          const phone = (g.identifierValue || '').toString()
          const norm = phone.replace(/\D/g,'')
          const explicit = ((g.firstName||g.first||'') + ' ' + (g.lastName||g.last||'')).trim()
          const csvRec = csvByPhone && (csvByPhone[norm] || csvByPhone[phone])
          const persist = persistentPhoneMap && persistentPhoneMap[norm]
          const idx = phoneIndexData && phoneIndexData[norm]
          // compute what would be used as display name by frontend fallback logic
          let display = explicit || (idx && idx.name) || (persist && (typeof persist === 'string' ? persist : persist.name)) || ''
          if(!display) display = phone
          console.log(`[guest-debug] phone=${phone} norm=${norm} explicit="${explicit || ''}" csv=${csvRec ? (csvRec.firstName||csvRec.fullname||'yes') : 'no'} persist=${persist ? (typeof persist==='string' ? persist : (persist.name||'obj')) : 'no'} index=${idx ? (idx.name||'yes') : 'no'} -> display="${display}"`)
        }catch(e){/* ignore per-guest debug error */}
      })
    }catch(e){ console.warn('guest-debug logging failed', e) }

    console.log(`[guests] returning ${final.length} guests (excludeFailed=${excludeFailed}, includeCheckedOut=${includeCheckedOut})`)
    res.json(final)
  })()
})

// Return the persistent phone->name map
app.get('/api/phone-map', (req,res)=>{
  res.json(persistentPhoneMap)
})

// Return the aggregated phone index (from all instay archives)
app.get('/api/phone-index', (req,res)=>{
  try{
    if(fs.existsSync(PHONE_INDEX_PATH)){
      const txt = fs.readFileSync(PHONE_INDEX_PATH, 'utf8')
      const j = JSON.parse(txt || '{}')
      return res.json(j)
    }
    // if local copy missing, try to build from Azure (async)
    if(azureAvailable && AZURE_CONN){
      updatePhoneIndexFromAzureArchives().then(idx=>{
        if(idx) return res.json(idx)
        return res.json({})
      }).catch(e=>{ return res.status(500).json({ error: String(e) }) })
      return
    }
    return res.json({})
  }catch(e){ return res.status(500).json({ error: String(e) }) }
})

// Add/update a mapping entry: { phone: '+123...', name: 'First Last' }
app.post('/api/phone-map', (req,res)=>{
  try{
    const { phone, name, checkoutDate } = req.body || {}
    if(!phone || !name) return res.status(400).json({ error: 'phone and name required' })
    const norm = phone.toString().replace(/\D/g,'')
    if(checkoutDate){
      persistentPhoneMap[norm] = { name: String(name).trim(), checkoutDate: String(checkoutDate).trim() }
    } else {
      persistentPhoneMap[norm] = String(name).trim()
    }
    savePersistentPhoneMap()
    return res.json({ ok:true, map: persistentPhoneMap })
  }catch(e){
    console.error('phone-map write error', e)
    return res.status(500).json({ error: String(e) })
  }
})

// POST /api/phone-map/sync
// Force a sync from the latest Azure `instay-archives` CSV and convert/overwrite
// local `persistentPhoneMap` entries into object form { name, checkoutDate }.
app.post('/api/phone-map/sync', async (req, res) => {
  try{
    if(!azureAvailable || !AZURE_CONN) return res.status(500).json({ error: 'azure_unavailable' })
    let records = await getCsvRecordsFromAzure()
    if(!records || !records.length){
      // fall back to local CSV if Azure CSV not available
      records = readGuests()
      if(!records || !records.length) return res.status(404).json({ error: 'no_csv_available' })
    }

    let updated = 0
    // Overwrite mappings with CSV values (use CSV as source of truth)
    records.forEach(r => {
      try{
        const firstName = r['First Name'] || r['Firstname'] || r['Given Name'] || r['Name'] || r['Guest Name'] || r['Guest'] || ''
        const lastName = r['Last Name'] || r['Lastname'] || r['Surname'] || r['Family Name'] || ''
        const phoneRaw = r['Ph.'] || r['Phone'] || r['Telephone'] || r['Mobile'] || r['Contact'] || ''
        const phone = phoneRaw ? phoneRaw.toString().replace(/\s+/g,'') : ''
        const norm = (phone||'').toString().replace(/\D/g,'')
        const fullname = ((firstName||'').toString().trim() + (lastName ? (' ' + (lastName||'').toString().trim()) : '')).trim()
        const checkoutRaw = r['Checkout Date'] || r['Check Out'] || r['Departure Date'] || r['Departure'] || r['CheckOut'] || ''
        const checkout = checkoutRaw ? String(checkoutRaw).trim() : ''
        if(norm && fullname){
          persistentPhoneMap[norm] = checkout ? { name: fullname, checkoutDate: checkout } : { name: fullname }
          updated++
        }
      }catch(e){}
    })

    // Convert any remaining string mappings to object form preserving name
    Object.keys(persistentPhoneMap).forEach(k => {
      const v = persistentPhoneMap[k]
      if(typeof v === 'string') persistentPhoneMap[k] = { name: v }
    })

    savePersistentPhoneMap()
    return res.json({ ok:true, updated, totalMappings: Object.keys(persistentPhoneMap).length })
  }catch(e){
    console.error('phone-map sync error', e)
    return res.status(500).json({ error: String(e) })
  }
})

// Also accept GET for convenience from browser (runs same sync logic)
app.get('/api/phone-map/sync', async (req, res) => {
  try{
    if(!azureAvailable || !AZURE_CONN) return res.status(500).json({ error: 'azure_unavailable' })
    let records = await getCsvRecordsFromAzure()
    if(!records || !records.length){
      records = readGuests()
      if(!records || !records.length) return res.status(404).json({ error: 'no_csv_available' })
    }

    let updated = 0
    records.forEach(r => {
      try{
        const firstName = r['First Name'] || r['Firstname'] || r['Given Name'] || r['Name'] || r['Guest Name'] || r['Guest'] || r.firstName || ''
        const lastName = r['Last Name'] || r['Lastname'] || r['Surname'] || r['Family Name'] || r.lastName || ''
        const phoneRaw = r['Ph.'] || r['Phone'] || r['Telephone'] || r['Mobile'] || r['Contact'] || r.identifierValue || ''
        const phone = phoneRaw ? phoneRaw.toString().replace(/\s+/g,'') : ''
        const norm = (phone||'').toString().replace(/\D/g,'')
        const fullname = ((firstName||'').toString().trim() + (lastName ? (' ' + (lastName||'').toString().trim()) : '')).trim()
        const checkoutRaw = r['Checkout Date'] || r['Check Out'] || r['Departure Date'] || r['Departure'] || r['CheckOut'] || r.checkoutDate || ''
        const checkout = checkoutRaw ? String(checkoutRaw).trim() : ''
        if(norm && fullname){
          persistentPhoneMap[norm] = checkout ? { name: fullname, checkoutDate: checkout } : { name: fullname }
          updated++
        }
      }catch(e){}
    })

    Object.keys(persistentPhoneMap).forEach(k => {
      const v = persistentPhoneMap[k]
      if(typeof v === 'string') persistentPhoneMap[k] = { name: v }
    })

    savePersistentPhoneMap()
    return res.json({ ok:true, updated, totalMappings: Object.keys(persistentPhoneMap).length })
  }catch(e){
    console.error('phone-map sync (GET) error', e)
    return res.status(500).json({ error: String(e) })
  }
})

// Check archive status for a given date (defaults to today)
app.get('/api/azure-archive-status', async (req,res)=>{
  try{
    if(!azureAvailable) return res.status(500).json({ error: 'azure sdk unavailable' })
    const date = req.query.date || new Date().toISOString().slice(0,10)
    const filename = `instay_output-${date}.csv`
    const containerName = 'instay-archives'
    const serviceClient = BlobServiceClient.fromConnectionString(AZURE_CONN)
    const containerClient = serviceClient.getContainerClient(containerName)
    try{
      const blobClient = containerClient.getBlobClient(filename)
      const props = await blobClient.getProperties()
      return res.json({ exists: true, filename, container: containerName, properties: { contentLength: props.contentLength, contentType: props.contentType, lastModified: props.lastModified } })
    }catch(e){
      // not found or other
      return res.json({ exists: false, filename, container: containerName })
    }
  }catch(e){
    console.error('azure status error', e)
    return res.status(500).json({ error: String(e) })
  }
})

// List recent archived blobs (optional limit)
app.get('/api/azure-archives', async (req,res)=>{
  try{
    if(!azureAvailable) return res.status(500).json({ error: 'azure sdk unavailable' })
    const limit = parseInt(req.query.limit||'50',10)
    const containerName = 'instay-archives'
    const serviceClient = BlobServiceClient.fromConnectionString(AZURE_CONN)
    const containerClient = serviceClient.getContainerClient(containerName)
    const items = []
    for await (const blob of containerClient.listBlobsFlat()){
      items.push({ name: blob.name, size: blob.properties && blob.properties.contentLength, lastModified: blob.properties && blob.properties.lastModified })
      if(items.length >= limit) break
    }
    return res.json({ container: containerName, items })
  }catch(e){
    console.error('azure list error', e)
    return res.status(500).json({ error: String(e) })
  }
})

// SSE endpoint: streams new messages as Bird messages arrive
app.get('/api/stream', (req,res)=>{
  res.writeHead(200, {
    'Content-Type': 'text/event-stream',
    'Cache-Control': 'no-cache',
    Connection: 'keep-alive'
  })
  res.write('\n')
  sseClients.add(res)
  // send a ping to keep connection alive every 20s
  const keep = setInterval(()=>{ try{ res.write('event: ping\ndata: {}\n\n') }catch(e){} }, 20000)
  // when client disconnects
  req.on('close', ()=>{
    clearInterval(keep)
    sseClients.delete(res)
  })
  // immediately attempt to broadcast any new messages
  // poller will broadcast new messages in background
})

// Get recent messages and filter by phone
app.get('/api/messages', async (req,res)=>{
  const phone = req.query.phone
  if(!phone) return res.json([])
  const url = `https://api.bird.com/workspaces/${WORKSPACE}/channels/${CHANNEL}/messages?limit=500`
  try{
    const r = await fetch(url, { headers: { Authorization: `AccessKey ${BIRD_API_KEY}`, Accept: 'application/json' } })
    const j = await r.json()
    const results = j.results||[]
    const filtered = results.filter(m=>{
      try{
        const rcv = m.receiver && m.receiver.contacts && m.receiver.contacts[0] && m.receiver.contacts[0].identifierValue
        const snd = m.sender && m.sender.contact && m.sender.contact.identifierValue
        return [rcv,snd].includes(phone)
      }catch(e){return false}
    })

    // Normalize messages for the UI
    const normalized = filtered.map(m=>{
      let text = ''
      let image = null
      let audio = null
      if(m.body){
        if(m.body.text && m.body.text.text) text = m.body.text.text
        else if(m.body.list && m.body.list.text) text = m.body.list.text
        else if(m.body.image){
          if(m.body.image.text) text = m.body.image.text
          const url = m.body.image.url || m.body.image.src
            || (m.body.image.media && m.body.image.media[0] && (m.body.image.media[0].url || m.body.image.media[0].mediaUrl))
            || m.body.image.mediaUrl
            || (m.body.image.images && m.body.image.images[0] && (m.body.image.images[0].mediaUrl || m.body.image.images[0].url || m.body.image.images[0].src))
          if(url){
            const lower = String(url||'').toLowerCase()
            // If URL looks like audio, treat it as audio rather than image
            if(/\.m4a$|\.mp3$|\.wav$|\.ogg$|audio\//.test(lower)){
              audio = { url, text: text || '' }
            } else {
              image = { url, text: text || '' }
            }
          }
        } else if(m.body.file || m.body.audio){
          // handle file/audio bodies (voice notes, attachments)
          try{
            const f = m.body.audio || m.body.file || {}
            if(f.text) text = f.text
            // also support Bird's file.files array shape (files: [{ mediaUrl, contentType }])
            const url = f.url || f.src
              || (f.media && f.media[0] && (f.media[0].url || f.media[0].mediaUrl))
              || f.mediaUrl
              || (f.files && f.files[0] && (f.files[0].mediaUrl || f.files[0].url || f.files[0].src))
              || null
            if(url){
              const lower = String(url||'').toLowerCase()
              if(/\.m4a$|\.mp3$|\.wav$|\.ogg$|audio\//.test(lower) || (f.mime && String(f.mime||'').toLowerCase().startsWith('audio')) || (f.files && f.files[0] && f.files[0].contentType && String(f.files[0].contentType||'').toLowerCase().startsWith('audio'))){
                audio = { url, text: text || '' }
              }
            }
          }catch(e){ /* ignore */ }
        } else text = JSON.stringify(m.body)
      }
      const deliveryStatus = m.status || m.deliveryStatus || (m.failure ? 'failed' : null)
      const deliveryReason = (m.failure && m.failure.description) || m.deliveryReason || null
      const bodyObj = image ? { image } : audio ? { audio } : { text }
      if(image && image.url){
        try{ bodyObj.image.proxyUrl = '/api/proxy-media?url=' + encodeURIComponent(image.url) }catch(e){}
      }
      if(audio && audio.url){
        try{ bodyObj.audio = bodyObj.audio || audio; bodyObj.audio.proxyUrl = '/api/proxy-media?url=' + encodeURIComponent(audio.url) }catch(e){}
      }
      // Fallback: Bird sometimes nests files under top-level `file` or under `body.type==='file'`
      if(!image && !audio){
        try{
          const topFile = m.file || (m.body && m.body.file) || (m.body && m.body.type === 'file' && m.body.file) || null
          if(topFile){
            const candidate = (topFile.files && topFile.files[0]) || (topFile.media && topFile.media[0]) || null
            const url = candidate && (candidate.mediaUrl || candidate.url || candidate.src) || null
            const contentType = candidate && (candidate.contentType || candidate.mime || '') || ''
            if(url){
              const lower = String(url||'').toLowerCase() + String(contentType||'').toLowerCase()
              if(/\.m4a$|\.mp3$|\.wav$|\.ogg$|audio\//.test(lower)){
                audio = { url, text: text || '' }
              } else {
                image = { url, text: text || '' }
              }
              if(audio && audio.url){ try{ bodyObj.audio = bodyObj.audio || audio; bodyObj.audio.proxyUrl = '/api/proxy-media?url=' + encodeURIComponent(audio.url) }catch(e){} }
              if(image && image.url){ try{ bodyObj.image = bodyObj.image || image; bodyObj.image.proxyUrl = '/api/proxy-media?url=' + encodeURIComponent(image.url) }catch(e){} }
            }
          }
        }catch(e){}
      }
      return {
        id: m.id,
        direction: m.direction || (m.sender && m.sender.connector ? 'outgoing' : 'incoming'),
        createdAt: m.createdAt,
        body: bodyObj,
        raw: m,
        deliveryStatus,
        deliveryReason
      }
    }).sort((a,b)=> new Date(a.createdAt) - new Date(b.createdAt))

    // Deduplicate messages by direction + normalized body/template name.
    // Normalize body text (lowercase, collapse whitespace, strip punctuation)
    // and keep only the first occurrence (earliest) to collapse repeated
    // template sends.
    const deduped = []
    const seen = new Set()
    const normalizeBody = (s) => {
      try{
        return String(s||'').toLowerCase().replace(/\s+/g,' ').replace(/["'`\.,\-\:\;\(\)\[\]\{\}\!\?]/g,'').trim()
      }catch(e){ return String(s||'').toLowerCase().trim() }
    }
    for(const m of normalized){
      try{
        const tmpl = m.raw && m.raw.template && m.raw.template.name ? String(m.raw.template.name).toLowerCase() : null
        const bodyText = (m.body && (m.body.text || (m.body.image && m.body.image.text) || (m.body.audio && m.body.audio.text) || (m.body.file && m.body.file.text))) ? String(m.body.text || (m.body.image && m.body.image.text) || (m.body.audio && m.body.audio.text) || (m.body.file && m.body.file.text) || '') : ''
        const normBody = normalizeBody(bodyText).slice(0,200)
        const sigBase = tmpl ? ('t:' + tmpl) : ('b:' + normBody)
        const sig = (m.direction||'') + '|' + sigBase
        if(seen.has(sig)) continue
        seen.add(sig)
        deduped.push(m)
      }catch(e){ deduped.push(m) }
    }

    res.json(deduped)
  }catch(err){
    res.status(500).json({error: String(err)})
  }
})

// Upload an image/file and return a URL
app.post('/api/upload-image', upload.single('file'), (req, res) => {
  try{
    if(!req.file) return res.status(400).json({ error: 'file required' })
    const url = `${req.protocol}://${req.get('host')}/uploads/${encodeURIComponent(req.file.filename)}`
    return res.json({ url })
  }catch(e){
    console.error('upload error', e)
    return res.status(500).json({ error: String(e) })
  }
})

// Proxy media from Bird to avoid CORS/private media issues
app.get('/api/proxy-media', async (req, res) => {
  const url = req.query.url
  if(!url) return res.status(400).json({ error: 'url query required' })
  try{
    const parsed = new URL(url)
    const allowedHosts = ['media.api.bird.com']
    if(!allowedHosts.includes(parsed.hostname)) return res.status(403).json({ error: 'forbidden host' })

    // derive a stable filename based on the url
    const hash = crypto.createHash('sha256').update(url).digest('hex')
    // try to preserve extension from path
    const extMatch = parsed.pathname.match(/\.([a-zA-Z0-9]{2,6})(?:\?|$)/)
    const ext = extMatch ? ('.' + extMatch[1]) : ''
    const filename = `${hash}${ext}`
    const filepath = path.join(uploadsDir, filename)

    if(fs.existsSync(filepath)){
      // serve cached file bytes
      const stat = fs.statSync(filepath)
      const contentType = require('mime-types').lookup(filepath) || 'application/octet-stream'
      res.setHeader('Content-Type', contentType)
      res.setHeader('Content-Length', stat.size)
      const stream = fs.createReadStream(filepath)
      return stream.pipe(res)
    }

    const r = await fetch(url, { headers: { Authorization: BIRD_API_KEY ? `AccessKey ${BIRD_API_KEY}` : undefined } })
    if(!r.ok) return res.status(502).json({ error: 'failed to fetch remote media', status: r.status })

    // determine extension from content-type if not present
    let remoteContentType = (r.headers && r.headers.get) ? r.headers.get('content-type') : null
    let finalExt = ext
    if(!finalExt && remoteContentType){
      const map = { 'image/jpeg': '.jpg', 'image/png': '.png', 'image/gif': '.gif', 'image/webp': '.webp' }
      if(map[remoteContentType]) finalExt = map[remoteContentType]
    }
    const finalFilename = finalExt && !filename.endsWith(finalExt) ? (hash + finalExt) : filename
    const finalPath = path.join(uploadsDir, finalFilename)

    const ab = await r.arrayBuffer()
    const buf = Buffer.from(ab)
    fs.writeFileSync(finalPath, buf)
    const stat = fs.statSync(finalPath)
    const contentType = require('mime-types').lookup(finalPath) || 'application/octet-stream'
    res.setHeader('Content-Type', contentType)
    res.setHeader('Content-Length', stat.size)
    const stream = fs.createReadStream(finalPath)
    return stream.pipe(res)
  }catch(e){
    console.error('proxy-media error', e)
    return res.status(500).json({ error: String(e) })
  }
})

// Send a plain text reply (wraps Bird payload used earlier)
app.post('/api/messages', async (req,res)=>{
  const { phone, text, imageUrl } = req.body
  if(!phone) return res.status(400).json({error:'phone required'})
  if(!text && !imageUrl) return res.status(400).json({error: 'text or imageUrl required'})
  const templateName = req.body.templateName || null
  // Prevent duplicate template/text sends: check recent messages for this phone
  try{
    // Strict block: if this is a template send and the phone is already in
    // the persistent sent-template blocklist, reject immediately.
    if(templateName){
      try{
        const norm = (phone||'').toString().replace(/\D/g,'')
        if(norm && sentTemplateBlocklist.has(norm)){
          console.log(`[send-blocklist] blocked attempt to send template=${templateName} to ${phone}`)
          return res.status(409).json({ error: 'template_already_sent', message: 'A template has already been sent to this number; further template sends are blocked.' })
        }
      }catch(e){/* ignore blocklist check errors */}
    }
    if(BIRD_API_KEY && WORKSPACE && CHANNEL){
      const recent = await fetchBirdMessages()
      const phoneNorm = (phone||'').toString().replace(/\D/g,'')
      const normalizeText = s => String(s||'').toString().trim().replace(/\s+/g,' ')
      const normalizeBody = s => String(s||'').toLowerCase().replace(/\s+/g,' ').replace(/["'`\.,\-\:\;\(\)\[\]\{\}\!\?]/g,'').trim()
      const same = recent.filter(m=>{
        try{
          const rcv = (m.receiver && m.receiver.contacts && m.receiver.contacts[0] && m.receiver.contacts[0].identifierValue) || ''
          const snd = (m.sender && m.sender.contact && m.sender.contact.identifierValue) || ''
          const rcvNorm = (rcv||'').toString().replace(/\D/g,'')
          const sndNorm = (snd||'').toString().replace(/\D/g,'')
          if(rcvNorm !== phoneNorm && sndNorm !== phoneNorm) return false
          const dir = m.direction || ((m.sender && m.sender.connector) ? 'outgoing' : 'incoming')
          // consider only outgoing (messages we sent)
          if(dir !== 'outgoing') return false
          // if templateName provided, prefer matching template names (case-insensitive)
          if(templateName && m.template && m.template.name && String(m.template.name).toLowerCase() === String(templateName).toLowerCase()) return true
          // otherwise compare normalized text bodies within recent timeframe (24 hours)
          const bodyText = (m.body && (m.body.text && m.body.text.text)) || (m.body && m.body.list && m.body.list.text) || (m.body && m.body.image && m.body.image.text) || ''
          if(text && bodyText){
            const a = normalizeBody(bodyText)
            const b = normalizeBody(text)
            if(a && b && a === b){
              const ageMs = Date.now() - (new Date(m.createdAt).getTime() || 0)
              if(ageMs >= 0 && ageMs <= (24*60*60*1000)) return true
            }
          }
          return false
        }catch(e){return false}
      })
      if(same && same.length){
        console.log(`[send-guard] duplicate prevented phone=${phone} template=${templateName||''} matches=${same.length}`)
        return res.status(409).json({ error: 'duplicate_message_detected', message: 'A similar message/template was recently sent to this number' })
      }
    }
  }catch(e){ /* ignore duplication-check failures */ }
  let payload = { receiver: { contacts: [{ identifierValue: phone }] } }
  if(imageUrl){
    payload.body = { type: 'image', image: { url: imageUrl, text: text || '' } }
  } else {
    payload.body = { type: 'text', text: { text } }
  }
  const url = `https://api.bird.com/workspaces/${WORKSPACE}/channels/${CHANNEL}/messages`
  try{
    const r = await fetch(url, { method:'POST', headers: { Authorization: `AccessKey ${BIRD_API_KEY}`, 'Content-Type':'application/json' }, body: JSON.stringify(payload) })
    // Try to parse JSON response from Bird, fall back to text
    let parsed = null
    try{
      parsed = await r.json()
    }catch(e){
      try{ parsed = await r.text() }catch(e2){ parsed = null }
    }
    // If we sent a template, persist the phone in the blocklist to prevent
    // any future template sends to the same number (strict blocking requirement).
    try{
      if(templateName && r && r.status && (r.status >= 200 && r.status < 300)){
        const norm = (phone||'').toString().replace(/\D/g,'')
        if(norm){
          sentTemplateBlocklist.add(norm)
          saveSentTemplateBlocklist()
          console.log(`[send-blocklist] added ${norm} after successful template send ${templateName}`)
        }
      }
    }catch(e){ /* ignore blocklist persistence errors */ }

    // After sending, the background poller will pick up and broadcast any new messages.
    if(parsed && typeof parsed === 'object') return res.status(r.status).json(parsed)
    return res.status(r.status).send(parsed)
  }catch(e){
    return res.status(500).json({error: String(e)})
  }
})

// Suggest reply using AI (OpenAI if configured, otherwise simple heuristic)
app.post('/api/suggest', async (req, res) => {
  const { phone, limit = 6, tone = 'friendly', mode = 'reply' } = req.body || {}
  if(!phone) return res.status(400).json({ error: 'phone required' })
  try{
    // fetch recent bird messages and filter by phone
    const url = `https://api.bird.com/workspaces/${WORKSPACE}/channels/${CHANNEL}/messages?limit=500`
    const r = await fetch(url, { headers: { Authorization: `AccessKey ${BIRD_API_KEY}`, Accept: 'application/json' } })
    const j = await r.json()
    const results = (j.results || []).filter(m=>{
      try{
        const rcv = m.receiver && m.receiver.contacts && m.receiver.contacts[0] && m.receiver.contacts[0].identifierValue
        const snd = m.sender && m.sender.contact && m.sender.contact.identifierValue
        return [rcv,snd].includes(phone)
      }catch(e){return false}
    }).sort((a,b)=> new Date(b.createdAt) - new Date(a.createdAt)).slice(0, limit)

    const convo = results.slice().reverse().map(m=>{
      const who = (m.direction||'incoming').toString() === 'incoming' ? 'guest' : 'you'
      const body = (m.body && ((m.body.text && m.body.text.text) || (m.body.list && m.body.list.text))) || ''
      return `${who}: ${body}`
    }).join('\n')

    // If OpenAI key present, call completions
    const OPENAI_KEY = process.env.OPENAI_API_KEY
    if(OPENAI_KEY){
      // find the last guest incoming message
      const lastGuestMsg = results.find(m=> (m.direction||'incoming') === 'incoming')
      const lastGuestText = lastGuestMsg ? ((lastGuestMsg.body && ((lastGuestMsg.body.text && lastGuestMsg.body.text.text) || (lastGuestMsg.body.list && lastGuestMsg.body.list.text))) || '') : ''

      if(mode === 'paraphrase'){
        // prompt to produce paraphrases of the guest's last message
        const sys = `You are an assistant that rewrites customer messages into alternative phrasings suitable for a hotel agent to echo back. Produce exactly 3 short paraphrases of the guest's LAST MESSAGE. Return a JSON object only with key \"suggestions\" -> array of strings.`
        const userPrompt = `Last guest message:\n"${lastGuestText}"\n\nReturn 3 different ways to rephrase this message for clarity or confirmation.`
        const messagesPayload = [ { role: 'system', content: sys }, { role: 'user', content: userPrompt } ]
        const resp = await fetch('https://api.openai.com/v1/chat/completions', {
          method: 'POST',
          headers: { 'Content-Type':'application/json', Authorization: `Bearer ${OPENAI_KEY}` },
          body: JSON.stringify({ model: 'gpt-3.5-turbo', messages: messagesPayload, max_tokens: 300, temperature: 0.6 })
        })
        const body = await resp.json()
        const txt = body && body.choices && body.choices[0] && body.choices[0].message && body.choices[0].message.content
        if(txt){
          try{ const parsed = JSON.parse(txt); if(parsed && Array.isArray(parsed.suggestions)) return res.json({ suggestions: parsed.suggestions.slice(0,3) }) }catch(e){}
          const m = txt.match(/\{[\s\S]*\}/)
          if(m){ try{ const parsed = JSON.parse(m[0]); if(parsed && Array.isArray(parsed.suggestions)) return res.json({ suggestions: parsed.suggestions.slice(0,3) }) }catch(e2){} }
          const lines = txt.split(/\n+/).map(s=>s.replace(/^\d+\.?\s*/, '').trim()).filter(Boolean)
          if(lines.length) return res.json({ suggestions: lines.slice(0,3) })
        }
      }

      // default mode: generate replies as before
      // try to detect guest name from messages
      let guestName = ''
      for(const m of results){
        const gname = (m && m.template && m.template.variables && (m.template.variables.firstname || m.template.variables.first_name)) || (m && m.receiver && m.receiver.contacts && m.receiver.contacts[0] && m.receiver.contacts[0].annotations && m.receiver.contacts[0].annotations.name) || null
        if(gname){ guestName = String(gname).trim(); break }
      }
      const sys = `You are a professional hotel front-desk customer service assistant. Read the conversation and produce exactly 3 short, polite reply suggestions tailored to the guest. Be concise, helpful, and maintain a friendly professional tone. Return a JSON object only, with a single key \"suggestions\" whose value is an array of strings. Do not include any extra text.`
      const userPrompt = `Guest name: ${guestName || 'Guest'}\nConversation:\n${convo}\n\nProvide 3 reply suggestions as a JSON object.`
      const messages = [ { role: 'system', content: sys }, { role: 'user', content: userPrompt } ]
      const resp = await fetch('https://api.openai.com/v1/chat/completions', {
        method: 'POST',
        headers: { 'Content-Type':'application/json', Authorization: `Bearer ${OPENAI_KEY}` },
        body: JSON.stringify({ model: 'gpt-3.5-turbo', messages, max_tokens: 300, temperature: 0.6 })
      })
      const body = await resp.json()
      const txt = body && body.choices && body.choices[0] && body.choices[0].message && body.choices[0].message.content
      if(txt){
        try{ const parsed = JSON.parse(txt); if(parsed && Array.isArray(parsed.suggestions)) return res.json({ suggestions: parsed.suggestions.slice(0,3) }) }catch(e){}
        const m = txt.match(/\{[\s\S]*\}/)
        if(m){ try{ const parsed = JSON.parse(m[0]); if(parsed && Array.isArray(parsed.suggestions)) return res.json({ suggestions: parsed.suggestions.slice(0,3) }) }catch(e2){} }
        const lines = txt.split(/\n+/).map(s=>s.replace(/^\d+\.?\s*/, '').trim()).filter(Boolean)
        if(lines.length) return res.json({ suggestions: lines.slice(0,3) })
      }
    }

    // Fallback heuristic: echo short acknowledgement + ask a question
    const lastGuest = results.find(m=> (m.direction||'incoming') === 'incoming')
    const lastText = lastGuest ? ((lastGuest.body && ((lastGuest.body.text && lastGuest.body.text.text) || (lastGuest.body.list && lastGuest.body.list.text))) || '') : ''
    const suggestion1 = `Hi, thanks for your message. ${ lastText ? 'We received: "' + (lastText.length>80? lastText.slice(0,77)+'...': lastText) + '".' : '' } How can I help?`
    const suggestion2 = `Hello, thanks for reaching out. I'll check and get back to you shortly.`
    return res.json({ suggestions: [suggestion1, suggestion2] })
  }catch(err){
    console.error('suggest error', err)
    return res.status(500).json({ error: String(err) })
  }
})

const PORT = process.env.PORT || 4000
app.listen(PORT, ()=>console.log('Server running on',PORT))
