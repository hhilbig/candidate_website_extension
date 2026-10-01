"""Extract static text from verified inventory responses without HTTP requests."""
import argparse
import gzip
import hashlib
import json
from pathlib import Path
import re
import shutil
import subprocess


def digest(data):
    return hashlib.sha256(data).hexdigest()


def extract(raw):
    from bs4 import BeautifulSoup
    soup = BeautifulSoup(raw, 'html.parser')
    encoding = soup.original_encoding
    for node in soup.select('script,style,noscript,template,svg,head,iframe,object,[hidden],[aria-hidden="true"]'):
        node.decompose()
    for node in soup.find_all(style=True):
        if node.attrs is not None and re.search(r'(display\s*:\s*none|visibility\s*:\s*hidden)', node.get('style',''), re.I):
            node.decompose()
    return ' '.join(soup.stripped_strings), encoding


def run_remote(payload):
    source = Path(payload['source'])
    data = source.read_bytes()
    if digest(data) != payload['source_sha256']:
        raise ValueError('Source hash differs')
    records = json.loads(data)
    selected = [r for r in records if r['saved_exact_response']]
    output = Path(payload['output'])
    if output.exists():
        raise ValueError('Refusing to replace existing text outputs')
    if shutil.disk_usage(output.parent).free < 8_000_000_000:
        raise ValueError('Insufficient volume headroom')
    # Verify every selected raw file before creating output files.
    texts, bindings = {}, []
    for row in selected:
        for item in row['exact_evidence']:
            sha = item['raw_sha256']
            if sha not in texts:
                path = Path(item['raw_path'])
                with (gzip.open(path,'rb') if path.suffix=='.gz' else path.open('rb')) as f:
                    raw = f.read(2_097_153)
                if len(raw)>2_097_152 or digest(raw)!=sha:
                    raise ValueError('Raw response exceeds limit or hash differs')
                text, encoding = extract(raw)
                texts[sha] = (text.encode(),encoding)
            bindings.append(dict(row_id=row['row_id'], candidate_url_id=row['candidate_url_id'],
                candidate_year_id=row['candidate_year_id'], timestamp=row['timestamp'],
                original_url=row['original'], raw_path=item['raw_path'], raw_sha256=sha))
    output.mkdir()
    for sha,(text,encoding) in texts.items():
        path = output/(sha+'.txt.gz')
        with path.open('xb') as f:
            f.write(gzip.compress(text,mtime=0))
        if gzip.decompress(path.read_bytes())!=text:
            raise ValueError('Saved text verification failed')
    for row in bindings:
        text,encoding = texts[row['raw_sha256']]
        row.update(text_path=str(output/(row['raw_sha256']+'.txt.gz')),
            text_sha256=digest(text), text_bytes=len(text), detected_encoding=encoding,
            status='nonempty_static_text' if text else 'empty_static_text',
            excerpt=text.decode()[:350])
    unique_bindings = {json.dumps(r,sort_keys=True):r for r in bindings}
    result = dict(source=payload['source'],source_sha256=payload['source_sha256'],
        extractor_script_sha256=payload['script_sha256'], selected_inventory_rows=len(selected),
        observed_inventory_rows=len({r['row_id'] for r in bindings}),
        unique_raw_files=len(texts), nonempty_text_files=sum(bool(t[0]) for t in texts.values()),
        decoded_text_bytes=sum(len(t[0]) for t in texts.values()),
        compressed_text_bytes=sum(p.stat().st_size for p in output.glob('*.txt.gz')),
        records=list(unique_bindings.values()), archive_requests=0,
        limitation='Static text only. CSS and scripts are not rendered; frames are not fetched. Nonempty text does not establish campaign relevance, ownership, or date validity.')
    if result['selected_inventory_rows']!=result['observed_inventory_rows']:
        raise ValueError('Missing row bindings')
    (output/'audit.json').write_text(json.dumps(result,indent=2)+'\n')
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--local-source',type=Path,required=True)
    parser.add_argument('--remote-source',required=True)
    parser.add_argument('--remote-output',required=True)
    parser.add_argument('--local-audit',type=Path,required=True)
    args=parser.parse_args()
    if args.local_audit.exists():
        raise ValueError('Refusing to replace local audit')
    payload=dict(source=args.remote_source,source_sha256=digest(args.local_source.read_bytes()),
        output=args.remote_output,script_sha256=digest(Path(__file__).read_bytes()))
    source=Path(__file__).read_text().split('\ndef main():')[0]
    proc=subprocess.run(['ssh','-o','BatchMode=yes','-o','ConnectTimeout=15','root@droplet',
        '/root/candidate_website_extension/.venv/bin/python','-'],
        input=source+'\nprint(json.dumps(run_remote('+repr(payload)+')))\n',
        text=True,capture_output=True,check=True,timeout=120)
    result=json.loads(proc.stdout)
    args.local_audit.write_text(json.dumps(result,indent=2)+'\n')
    print(json.dumps({k:v for k,v in result.items() if k!='records'},indent=2))


if __name__=='__main__':
    main()
