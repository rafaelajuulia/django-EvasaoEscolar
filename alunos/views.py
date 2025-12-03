from django.shortcuts import render, redirect, get_object_or_404
from .models import Aluno
from .forms import AlunoForm


def listar_alunos(request):
    alunos = Aluno.objects.all()
    return render(request, 'alunos/listar.html', {'alunos': alunos})


def cadastrar_aluno(request):
    form = AlunoForm(request.POST or None)
    if form.is_valid():
        form.save()
        return redirect('listar_alunos')
    return render(request, 'alunos/cadastrar.html', {'form': form})


def editar_aluno(request, id):
    aluno = get_object_or_404(Aluno, id=id)
    form = AlunoForm(request.POST or None, instance=aluno)
    if form.is_valid():
        form.save()
        return redirect('listar_alunos')
    return render(request, 'alunos/cadastrar.html', {'form': form})


def excluir_aluno(request, id):
    aluno = get_object_or_404(Aluno, id=id)
    aluno.delete()
    return redirect('listar_alunos')