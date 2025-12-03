from django.urls import path
from .views import listar_alunos, cadastrar_aluno, editar_aluno, excluir_aluno

urlpatterns = [
    path('', listar_alunos, name='listar_alunos'),
    path('novo/', cadastrar_aluno, name='cadastrar_aluno'),
    path('editar/<int:id>/', editar_aluno, name='editar_aluno'),
    path('excluir/<int:id>/', excluir_aluno, name='excluir_aluno'),
]