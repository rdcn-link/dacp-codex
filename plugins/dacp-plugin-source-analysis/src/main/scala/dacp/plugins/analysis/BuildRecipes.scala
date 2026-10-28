package dacp.plugins.analysis

import dacp.operator.SourceBuildRecipe
import java.nio.file.Path

private object ArtifactEntrypoint {
  def check(path:Path,entries:Seq[String]):Unit={val zip=new java.util.zip.ZipFile(path.toFile);try require(entries.exists(e=>zip.getEntry(e)!=null),"entrypoint is missing from the built artifact")finally zip.close()}
}

final class PythonBuildRecipe extends SourceBuildRecipe {
  def language="python"
  def executor="python"
  def artifactFilename="operator.zip"
  def verifyArtifact(path:Path,entrypoint:String):Unit=ArtifactEntrypoint.check(path,Seq(entrypoint.replace('.','/')+".pyc",entrypoint.replace('.','/')+"/__main__.pyc"))
  def command=Seq("/usr/local/bin/python","-c","""import pathlib, zipfile, py_compile, sys, ast, json
root=pathlib.Path('/source/src')
files=sorted(root.rglob('*'))
assert root.is_dir() and any(p.suffix=='.py' for p in files), 'src/ must contain Python sources'
modules=[]
with zipfile.ZipFile('/output/operator.zip','w',zipfile.ZIP_DEFLATED) as z:
 for p in files:
  if p.is_file():
   name=str(p.relative_to(root))
   if p.suffix=='.py':
    tree=ast.parse(p.read_bytes(),filename=name)
    if any(isinstance(n,ast.FunctionDef) and n.name=='process' for n in tree.body): modules.append(name[:-3].replace('/','.'))
    py_compile.compile(str(p), cfile='/tmp/check.pyc', dfile=name, doraise=True, invalidation_mode=py_compile.PycInvalidationMode.UNCHECKED_HASH)
    z.write('/tmp/check.pyc', name+'c')
   elif name!='.dacp-build.json': z.write(p,name)
 z.writestr('.dacp-build.json',json.dumps({'python_process_modules':modules}))
with open('/output/operator.zip','rb') as f: sys.stdout.buffer.write(f.read())
""")
}
final class JavaBuildRecipe extends SourceBuildRecipe {
  def language="java"
  def executor="jvm"
  def artifactFilename="operator.jar"
  def verifyArtifact(path:Path,entrypoint:String):Unit=ArtifactEntrypoint.check(path,Seq(entrypoint.replace('.','/')+".class"))
  def command=Seq("/opt/java/bin/java","-cp","/builder/helper.jar:/opt/lib/*","dacp.repository.build.JvmCompiler","java")
}
final class ScalaBuildRecipe extends SourceBuildRecipe {
  def language="scala"
  def executor="jvm"
  def artifactFilename="operator.jar"
  def verifyArtifact(path:Path,entrypoint:String):Unit=ArtifactEntrypoint.check(path,Seq(entrypoint.replace('.','/')+".class"))
  def command=Seq("/opt/java/bin/java","-cp","/builder/helper.jar:/opt/lib/*","dacp.repository.build.JvmCompiler","scala")
}
