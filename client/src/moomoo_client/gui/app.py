import kivy
from kivy.app import App
from kivy.uix.label import Label

kivy.require("2.2.0")


class MyApp(App):
    def build(self):
        return Label(text="Hello world")
