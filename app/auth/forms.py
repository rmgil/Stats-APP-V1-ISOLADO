"""Authentication forms using Flask-WTF"""
from flask_wtf import FlaskForm
from wtforms import StringField, PasswordField, BooleanField, SubmitField
from wtforms.validators import DataRequired, Email, Length, EqualTo, ValidationError

class LoginForm(FlaskForm):
    """User login form"""
    email = StringField('Email', 
                       validators=[DataRequired(), Email()],
                       render_kw={"placeholder": "Introduza o seu email"})
    password = PasswordField('Palavra-passe', 
                           validators=[DataRequired()],
                           render_kw={"placeholder": "Introduza a sua palavra-passe"})
    remember_me = BooleanField('Lembrar-me')
    submit = SubmitField('Entrar')

class RegistrationForm(FlaskForm):
    """User registration form"""
    username = StringField('Nome de utilizador', 
                         validators=[DataRequired(), Length(min=3, max=20)],
                         render_kw={"placeholder": "Escolha um nome de utilizador"})
    email = StringField('Email', 
                       validators=[DataRequired(), Email()],
                       render_kw={"placeholder": "Introduza o seu email"})
    invite_code = StringField('Código de Convite (opcional)', 
                            validators=[Length(max=100)],
                            render_kw={"placeholder": "Introduza o código de convite se tiver"})
    password = PasswordField('Palavra-passe', 
                           validators=[DataRequired(), Length(min=6)],
                           render_kw={"placeholder": "Mínimo 6 caracteres"})
    confirm_password = PasswordField('Confirmar Palavra-passe',
                                    validators=[DataRequired(), EqualTo('password')],
                                    render_kw={"placeholder": "Introduza a palavra-passe novamente"})
    submit = SubmitField('Registar')