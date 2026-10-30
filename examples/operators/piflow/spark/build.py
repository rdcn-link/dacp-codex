#!/usr/bin/env python3
"""Compile the original scientific algorithms and one explicitly selected DACP entrypoint."""
import argparse
import json
from pathlib import Path
import shutil
import subprocess
import zipfile

HERE = Path(__file__).resolve().parent
HOME = HERE.parent
ROOT = HOME.parents[2]


def build(spark_home, name):
    specs = json.loads((HOME / 'specs.json').read_text())
    spec = specs[name]
    if spec.get('executor') != 'jvm':
        raise ValueError('this build helper requires a JVM operator specification')
    inventory = json.loads((HOME / 'inventory.json').read_text())
    item = next(x for x in inventory['operators'] if x['upstream_class'].endswith('.' + spec['class']))
    target = HOME / 'target/spark' / name
    src = target / 'src'
    classes = target / 'classes'
    shutil.rmtree(target, ignore_errors=True)
    src.mkdir(parents=True)
    classes.mkdir()
    upstream = HOME / 'upstream/piflow-bundle/src/main'
    util = upstream / 'scala/cn/piflow/bundle/flux/util'
    sources = []
    for p in [*util.glob('*.scala'), HOME / 'upstream' / item['source']]:
        text = p.read_text()
        # Derby's SQL PI function was imported as a constant; use the actual constant.
        text = text.replace('import org.apache.derby.catalog.SystemProcedures.PI', 'import scala.math.{Pi => PI}')
        # Scala 2.13 Seq is immutable; Row.fromSeq requires an immutable snapshot.
        text = text.replace('Row.fromSeq(arr)', 'Row.fromSeq(arr.toVector)')
        text = text.replace('.stripSuffix(', '.toString.stripSuffix(')
        text = text.replace('import spark.sqlContext.implicits._', 'import spark.implicits._')
        # Row.toString includes brackets and prevented any threshold from matching.
        if p.name == 'FluxUtil_1.scala':
            text = text.replace('thresholdFields.append(x.toString.toLowerCase.trim)',
                                'thresholdFields.append(x.getString(0).toLowerCase.trim)')
        if p.name == 'FluxInterpolationUtil_4.scala':
            # Keep earlier removals when processing more than one field.
            text = text.replace('df = originDF.drop(field)', 'df = df.drop(field)')
        if p.name == 'FrontAndBackValuesAreValid.scala':
            text = text.replace('ForwardAndBackwardInterpolation(spark,df,fields,-9999)',
                                'ForwardAndBackwardInterpolation(spark,df,fields,qualityControlMarkerValue.toInt)')
        if p.name == 'CurrentAndPostValueInterpolation.scala':
            text = text.replace('fields,-9999)', 'fields,qualityControlMarkerValue.toInt)')
        if p.name == 'InterpolationMetro.scala':
            # Signed differences are required for decreasing as well as increasing series.
            text = text.replace('math.abs(d1.toString.toDouble-d4)', '(d4-d1.toString.toDouble)')
            text = text.replace('math.abs(d2-d5.toString.toDouble)', '(d5.toString.toDouble-d2)')
        if p.name == 'FluxDataCorrectionUtil_2.scala':
            text = text.replace('.repartition(20)', '.repartition(2)')
            # The original planar-fit SQL accidentally selected CO2 four times.
            for field, index in [('LeField', 1), ('HsField', 2), ('u_starField', 3)]:
                text = text.replace("',')[0],5)  as ${" + field + "}",
                                    "',')[" + str(index) + "],5)  as ${" + field + "}")
        if p.name == 'FluxOutlierHandlingUtil_3.scala':
            text = text.replace('    val statisticalType = "stddev"', '    // use the requested statisticalType')
            text = text.replace('|select id,', '|select id')
            text = text.replace('where contrast_flag<> $flagValue', 'where b.$field<> $flagValue')
        if p.name == 'PM_HS.scala':
            text = text.replace('x.get("rho_a_Avg")', 'x.get(rho_a_Avg)')
        path = src / p.name
        path.write_text(text)
        sources.append(path)
    jars = spark_home / 'deps/jars'
    sdk = ROOT / 'dacp-operator-sdk/jvm/target/scala-2.13/dacp-operator-sdk_2.13-0.1.0-SNAPSHOT.jar'
    if not sdk.is_file():
        raise ValueError('build dacp-operator-sdk first')
    api = ROOT / 'dacp-plugin-api/target/scala-2.13/dacp-plugin-api_2.13-0.1.0-SNAPSHOT.jar'
    cp = str(jars / '*') + ':' + str(sdk) + ':' + str(api)
    java = upstream / 'java/cn/piflow/bundle/FluxJavaUtil/Theleastsquaremethod.java'
    subprocess.run(['javac', '-cp', cp, '-d', str(classes), str(java)], check=True)
    wrapper = src / 'Entrypoint.scala'
    ports = ','.join(json.dumps(k) + '->' + json.dumps(v) for k, v in spec['upstream_ports'].items())
    params = ','.join(json.dumps(k) for k in spec['params'])
    # A plain main facade deliberately excludes Spark operators from automatic JVM
    # fusion: each owns a SparkContext and its temporary storage lifecycle.
    wrapper.write_text('package dacp.ecoflow\nobject Op_' + name + ' {\n'
        ' def main(args:Array[String]):Unit={\n'
        '  val op=new org.apache.spark.sql.dacp.EcoFlowOperator('
        + json.dumps(item['upstream_class']) + ',Map(' + ports + '),Set(' + params + ')){}\n'
        '  val protocol=System.out;System.setOut(System.err)\n'
        '  try {if(args.headOption.contains("infer")) dacp.operator.OperatorRunner.infer(args,protocol)(op.inferOutputs)\n'
        '       else dacp.operator.OperatorRunner.run(args,protocol)(op.process)}\n'
        '  finally System.setOut(protocol)\n }\n}\n')
    subprocess.run(['java', '-cp', str(jars / '*'), 'scala.tools.nsc.Main', '-classpath', cp + ':' + str(classes), '-d', str(classes),
                    str(HERE / 'Contracts.scala'), str(HERE / 'EcoFlowOperator.scala'), str(wrapper), *map(str, sources)], check=True)
    artifact = target / ('dacp-ecoflow-' + name + '-1.0.jar')
    with zipfile.ZipFile(artifact, 'w', zipfile.ZIP_DEFLATED) as z:
        for p in classes.rglob('*.class'):
            z.write(p, p.relative_to(classes).as_posix())
        for dependency in (sdk, api):
            with zipfile.ZipFile(dependency) as dependency_zip:
                for n in dependency_zip.namelist():
                    if n.endswith('.class'):
                        z.writestr(n, dependency_zip.read(n))
    print(artifact, flush=True)
    return artifact


if __name__ == '__main__':
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('name')
    p.add_argument('--spark-home', type=Path, required=True)
    args = p.parse_args()
    build(args.spark_home, args.name)
