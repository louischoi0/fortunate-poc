import crypto
import sys
sys.modules['Crypto'] = crypto

from OpenSSL.crypto import load_privatekey, FILETYPE_PEM, sign, load_publickey, verify, X509
from Crypto.Hash import SHA256 as SHA
from OpenSSL import crypto
import sys
 
pkey = crypto.PKey()                      
pkey.generate_key(crypto.TYPE_RSA, 1024)                    #RSA 형식의 키 생성
 
# 공개키, 개인키 생성
with open("public.pem",'ab+') as f:
    f.write(crypto.dump_publickey(crypto.FILETYPE_PEM, pkey))
 
with open("private.pem",'ab+') as f:
    f.write(crypto.dump_privatekey(crypto.FILETYPE_PEM, pkey))
 
# 공개키, 개인키 읽어오기 
with open("private.pem",'rb+') as f:                                     
    priv_key = crypto.load_privatekey(crypto.FILETYPE_PEM, f.read())
 
with open("public.pem",'rb+') as f:
    pub_key = crypto.load_publickey(crypto.FILETYPE_PEM, f.read())
    
# 데이터 해시화 
content = '_message'                                         #가져와야 하는것
_hash = SHA.new(content.encode('utf-8')).digest()            # 데이터 해시화 = 프레임 해쉬화 데이터로 대체할 것
 
#개인키로 전자서명
_sig =  sign(priv_key, _hash, 'sha256')                      # 개인키로 해시화 되어 있는걸 전자서명
 
 
x509 = X509()                                                # 인증서를 사용할 수 있도록 하는 메서드 제공
x509.set_pubkey(pub_key)
 
#비교 대상
cc2 = '_message'
t_hash = SHA.new(cc2.encode('utf-8')).digest()
 

class FortunateSignature:
    def __init__(self, message, pubkey, *args, **kwargs):
        self.message = message
        self.pubkey = pubkey

    def sign(self, private_key, *args, **kwargs):
        _hash = SHA.new(self.message).digest()
        _sign = sign(private_key, _hash, 'sha256')
     
    def verity(self, message, private_key, *args, **kwargs):
        x509 = X509()
        x509.set_pubkey(self.pubkey) 
        
msg = "abcdef".encode('utf8')

try :
    verify(x509, _sig, t_hash, 'sha256')
    print("okay!!")
except :
    print("wrong!!")
    sys.exit(1) 
