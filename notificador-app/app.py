from confluent_kafka import Consumer, KafkaError
import json
import smtplib
from email.message import EmailMessage
import os

EMAIL_REMETENTE = os.environ.get("EMAIL_REMETENTE", "victorpst04@gmail.com")
SENHA_APP = os.environ.get("SENHA_APP", "xxx")
#substituir pela senha do e-mail do remetente, aqui deixei o meu proprio e-mail já
#que era apenas pra teste do kafka.
EMAIL_DESTINO = os.environ.get("EMAIL_DESTINO", "victorpst04@gmail.com")

def enviar_email(arquivo, operacao):
    msg = EmailMessage()
    msg['Subject'] = 'Aviso do Kafka: Imagem processada!'
    msg['From'] = EMAIL_REMETENTE
    msg['To'] = EMAIL_DESTINO
    msg.set_content(f'Olá! O arquivo {arquivo} foi {operacao} com sucesso.')

    try:
        with smtplib.SMTP_SSL('smtp.gmail.com', 465) as smtp:
            smtp.login(EMAIL_REMETENTE, SENHA_APP)
            smtp.send_message(msg)
        print(f"E-mail enviado com sucesso para {EMAIL_DESTINO}!")
    except Exception as e:
        print(f"Erro ao enviar e-mail: {e}")


c = Consumer({
    'bootstrap.servers': 'kafka1:19091,kafka2:19092,kafka3:19093',
    'group.id': 'notificador-group',
    'client.id': 'notificador-client',
    'enable.auto.commit': True,
    'session.timeout.ms': 6000,
    'default.topic.config': {'auto.offset.reset': 'smallest'}
})

c.subscribe(['notificacao'])

print("Iniciando o Notificador de E-mail...")

try:
    while True:
        msg = c.poll(1.0)
        if msg is None:
            continue
        elif not msg.error():
            dados = json.loads(msg.value())
            print(f"Recebido aviso do Kafka: {dados}")
            enviar_email(dados['arquivo'], dados['operacao'])
            
        elif msg.error().code() == KafkaError._PARTITION_EOF:
            pass 
        else:
            print(f"Erro no Kafka: {msg.error().str()}")

except KeyboardInterrupt:
    pass
finally:
    c.close()