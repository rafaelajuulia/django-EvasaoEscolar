from django.db import models

class Aluno(models.Model):
    nome = models.CharField(max_length=255)
    email = models.EmailField(unique=True)
    idade = models.PositiveIntegerField()
    serie = models.CharField(max_length=50)

    motivo_evasao = models.TextField(blank=True, null=True)
    risco_evasao = models.BooleanField(default=False)

    data_cadastro = models.DateTimeField(auto_now_add=True)

    def __str__(self):
        return self.nome
