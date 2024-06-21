REGEXP_REPLACE(
    REGEXP_REPLACE(
      REGEXP_REPLACE(
        TRANSLATE(x,  "ŠŽšžŸÀÁÂÃÄÅÇÈÉÊËÌÍÎÏÐÑÒÓÔÕÖÙÚÛÜÝàáâãäåçèéêëìíîïðñòóôõöùúûüýÿ", "SZszYAAAAAACEEEEIIIIDNOOOOOUUUUYaaaaaaceeeeiiiidnooooouuuuyy"),
      ",|;|\\/|\\.|'",""),
    '"|\\"',""),
  r'[^a-zA-Z0-9]', ' ')
