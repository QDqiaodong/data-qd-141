
import copy,json,unittest,tempfile,subprocess,sys,hashlib,random,itertools
from pathlib import Path
from fractions import Fraction
from sequence_align.model import analyze
ROOT=Path(__file__).resolve().parents[1]
def sample():return json.loads((ROOT/'fixtures/input.json').read_text())
class Business(unittest.TestCase):
    def bad(self,d):
        with self.assertRaises((ValueError,TypeError)):analyze(d)
    def test_unknown_fields(self):
        d=sample();d['unknown']=1;self.bad(d)

    def replay(self,a,b,r):
        i=j=0;out=[]
        for o in r['operations']:
            self.assertEqual((o['before_index'],o['after_index']),(i,j))
            if o['source'] is not None:self.assertEqual(o['source'],a[i]);i+=1
            if o['target'] is not None:out.append(o['target']);j+=1
        self.assertEqual(i,len(a));self.assertEqual(out,b);self.assertEqual(r['distance'],sum(o['op']!='match' for o in r['operations']))
    def test_sample(self):
        d=sample();r=analyze(d);self.assertEqual(r['distance'],2);self.replay(d['before'],d['after'],r)
    def test_empty(self):self.assertEqual(analyze({'before':[],'after':[]})['distance'],0)
    def test_insert_delete(self):
        for a,b in [([],['a','b']),(['a','b'],[])]:
            r=analyze({'before':a,'after':b});self.assertEqual(r['distance'],2);self.replay(a,b,r)
    def test_replace(self):
        r=analyze({'before':['a'],'after':['b']});self.assertEqual(r['operations'][0]['op'],'replace')
    def test_repeated(self):
        d={'before':['a','a','b'],'after':['a','b','b']};r=analyze(d);self.assertEqual(r['distance'],1);self.replay(d['before'],d['after'],r)
    def test_exhaustive_short(self):
        from functools import lru_cache
        @lru_cache(None)
        def oracle(a,b):
            if not a:return len(b)
            if not b:return len(a)
            return min(oracle(a[1:],b)+1,oracle(a,b[1:])+1,oracle(a[1:],b[1:])+(a[0]!=b[0]))
        words=[x for n in range(4) for x in itertools.product('ab',repeat=n)]
        for a in words:
            for b in words:
                r=analyze({'before':list(a),'after':list(b)});self.assertEqual(r['distance'],oracle(a,b));self.replay(a,list(b),r)
    def test_bound(self):self.bad({'before':['a']*501,'after':[]})

class Command(unittest.TestCase):
    def call(self,source,output):
        return subprocess.run([sys.executable,'-m','sequence_align.cli','--source',str(source),'--output',str(output)],capture_output=True,text=True)
    def test_cli_replay(self):
        with tempfile.TemporaryDirectory() as td:
            out=Path(td)/'out.json';src=ROOT/'fixtures/input.json';r=self.call(src,out);self.assertEqual(r.returncode,0,r.stderr);first=out.read_bytes();self.assertEqual(json.loads(first)['source_sha256'],hashlib.sha256(src.read_bytes()).hexdigest());self.assertEqual(self.call(src,out).returncode,0);self.assertEqual(first,out.read_bytes())
    def test_failure_preserves(self):
        with tempfile.TemporaryDirectory() as td:
            src=Path(td)/'bad.json';out=Path(td)/'out.json';out.write_text('prior')
            for body in ['{}','{"a":1,"a":2}','NaN']:
                src.write_text(body);self.assertEqual(self.call(src,out).returncode,2);self.assertEqual(out.read_text(),'prior')
    def test_same_path_and_symlink(self):
        with tempfile.TemporaryDirectory() as td:
            src=Path(td)/'in.json';src.write_bytes((ROOT/'fixtures/input.json').read_bytes());before=src.read_bytes();self.assertEqual(self.call(src,src).returncode,2);self.assertEqual(src.read_bytes(),before);out=Path(td)/'link.json';out.symlink_to(src);self.assertEqual(self.call(src,out).returncode,2);self.assertEqual(src.read_bytes(),before)
