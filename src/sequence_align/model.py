from .common import require,ident,rows
from .alignment import align

def analyze(data):
    require(data,'before after');a=[ident(t) for t in rows(data['before'],500)];b=[ident(t) for t in rows(data['after'],500)]
    return align(a,b)
