entry = db.files.findOne( { pnfsid: id } )

if ( entry ) {
  if ( entry.archiveUrl ) {
    print(entry.archiveUrl)
  }
} else {
  db.files.insert( { pnfsid: id, store: ystore, group: ygroup } )
}

