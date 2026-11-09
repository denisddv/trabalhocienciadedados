import random,sys,unittest
from datetime import datetime,timezone
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'producer'))
from gerar_eventos import gerar_evento
class TestGerador(unittest.TestCase):
    def test_valores(self):
        e=gerar_evento(5,random.Random(2026))
        self.assertEqual(e['id_pedido'],'PED-00000005')
        self.assertGreater(e['quantidade'],0)
    def test_atraso(self):
        e=gerar_evento(1,random.Random(1),True)
        segundos=(datetime.now(timezone.utc)-datetime.fromisoformat(e['event_time'])).total_seconds()
        self.assertTrue(230<segundos<250)
if __name__=='__main__':unittest.main()
