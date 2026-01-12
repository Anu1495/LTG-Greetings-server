#!/usr/bin/env node
/*
Prune Azure instay-archives container so only the latest file
for each date-stamp (YYYY-MM-DD or YYYYMMDD) is kept.

Usage:
  node prune_instay_archives.js [--delete] [--container=instay-archives] [--dry-run=false]

By default the script runs in dry-run mode and will only print which blobs
would be deleted. Pass `--delete` or `--dry-run=false` to perform deletions.
Requires `AZURE_STORAGE_CONNECTION_STRING` env var to be set.
*/

const { BlobServiceClient } = require('@azure/storage-blob')
const dotenv = require('dotenv')
const path = require('path')

// load .env from workspace root if present
try{ dotenv.config({ path: path.join(process.cwd(),'..','.env') }) }catch(e){}

const AZURE_CONN = process.env.AZURE_STORAGE_CONNECTION_STRING
if(!AZURE_CONN){
  console.error('Missing AZURE_STORAGE_CONNECTION_STRING environment variable. Exiting.')
  process.exitCode = 2
  return
}

const argv = require('minimist')(process.argv.slice(2))
const containerName = argv.container || 'instay-archives'
const doDelete = !!(argv.delete || argv['delete'] || argv['do-delete']) || String(argv['dry-run']||'true').toLowerCase() === 'false'
const dryRun = !doDelete

function deriveDateKey(name){
  if(!name) return null
  // match YYYY-MM-DD or YYYYMMDD
  const m1 = String(name).match(/(20\d{2}-[01]\d-[0-3]\d)/)
  if(m1) return m1[1].replace(/-/g,'')
  const m2 = String(name).match(/(20\d{2}[01]\d[0-3]\d)/)
  if(m2) return m2[1]
  return null
}

;(async ()=>{
  try{
    const serviceClient = BlobServiceClient.fromConnectionString(AZURE_CONN)
    const containerClient = serviceClient.getContainerClient(containerName)
    const exists = await containerClient.exists()
    if(!exists){
      console.error(`Container ${containerName} does not exist. Nothing to do.`)
      return
    }

    const blobs = []
    for await (const b of containerClient.listBlobsFlat()){
      blobs.push({ name: b.name, lastModified: b.properties && b.properties.lastModified })
    }
    if(!blobs.length){
      console.log(`No blobs found in ${containerName}`)
      return
    }

    const groups = {}
    blobs.forEach(b => {
      const key = deriveDateKey(b.name)
      if(!key) return
      groups[key] = groups[key] || []
      groups[key].push(b)
    })

    const keys = Object.keys(groups).sort()
    if(!keys.length){
      console.log('No date-stamped blobs found to prune.')
      return
    }

    let deletions = []
    keys.forEach(k => {
      const arr = groups[k]
      // sort by lastModified desc, keep first
      arr.sort((a,b)=> new Date(b.lastModified) - new Date(a.lastModified))
      const keep = arr[0]
      const remove = arr.slice(1)
      if(remove.length){
        deletions = deletions.concat(remove.map(r=>({ dateKey: k, keep: keep.name, remove: r.name, removedLastModified: r.lastModified })))
      }
    })

    if(!deletions.length){
      console.log('Nothing to delete; every date has only one (latest) blob.')
      return
    }

    console.log(`Found ${deletions.length} blobs to remove across ${Object.keys(groups).length} dates`)
    deletions.forEach(d => console.log(`Date ${d.dateKey}: keep=${d.keep}  remove=${d.remove}  lastModified=${d.removedLastModified}`))

    if(dryRun){
      console.log('\nDry-run mode; no blobs were deleted. Re-run with `--delete` to perform deletions.')
      return
    }

    // perform deletions
    for(const d of deletions){
      try{
        const blobClient = containerClient.getBlobClient(d.remove)
        await blobClient.deleteIfExists()
        console.log(`Deleted: ${d.remove}`)
      }catch(e){
        console.warn(`Failed to delete ${d.remove}:`, e.message || e)
      }
    }
    console.log('Prune complete.')
  }catch(e){
    console.error('Prune error', e)
    process.exitCode = 1
  }
})()
