from django.contrib import admin
from .models import Aluno

@admin.register(Aluno)
class AlunoAdmin(admin.ModelAdmin):
    list_display = ('nome', 'email', 'idade', 'serie', 'risco_evasao')
    search_fields = ('nome', 'email')
    list_filter = ('serie', 'risco_evasao')
