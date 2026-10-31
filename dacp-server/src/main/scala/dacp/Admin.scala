package dacp

object Admin {
  val commands=dacp.union.CredentialAdmin.commands ++ Set("list-history","get-history")
  def main(args:Array[String]):Unit=run("dacp-admin",args)
  def run(program:String,args:Array[String]):Unit={
    val usage=s"""$program create-account DIRECTORY --subject NAME [--role reader|runner] [--expires-in SECONDS]
$program create-token DIRECTORY --subject NAME [--expires-in SECONDS]
$program grant-union DIRECTORY --subject NAME --union-id ID --fingerprint SHA256 [--expires-in SECONDS]
$program list-credentials DIRECTORY
$program revoke-credential|rotate-credential DIRECTORY CREDENTIAL_ID
$program list-subjects DIRECTORY
$program set-roles DIRECTORY SUBJECT reader|runner|none
$program presign-get DIRECTORY ACCESS_KEY HTTPS_OBJECT_URL [SECONDS]
$program list-history DIRECTORY [OFFSET LIMIT]
$program get-history DIRECTORY RECORD_ID"""
    args.toList match {
      case Nil | List("--help")=>println(usage)
      case "list-history" :: rest=>ExecutionHistoryAdmin.main(("list" :: rest).toArray)
      case "get-history" :: rest=>ExecutionHistoryAdmin.main(("get" :: rest).toArray)
      case command :: _ if dacp.union.CredentialAdmin.commands(command)=>dacp.union.CredentialAdmin.main(args)
      case _=>throw new IllegalArgumentException(usage)
    }
  }
}
