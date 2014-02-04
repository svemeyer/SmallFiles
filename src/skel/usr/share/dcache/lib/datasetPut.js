entry = db.files.findOne( { pnfsid: bfid } )

if ( entry ) {
  if ( entry.archiveUrl ) {
    print(entry.archiveUrl)
  }
} else {
  db.files.insert( { pnfsid: bfid, store: ystore, group: ygroup, archiveUrl: null } )
}

