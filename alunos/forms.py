from django import forms
from .models import Aluno

class AlunoForm(forms.ModelForm):
    class Meta:
        model = Aluno
        fields = '__all__'
        widgets = {
            'motivo_evasao': forms.Textarea(attrs={'rows': 4, 'placeholder': 'Descreva o motivo (opcional)'}),
            'risco_evasao': forms.RadioSelect(choices=[(True, 'Sim'), (False, 'Não')]),
        }
