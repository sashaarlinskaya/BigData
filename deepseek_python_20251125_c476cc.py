#!/usr/bin/env python3
"""
Скрипт для установки необходимых зависимостей
"""
import subprocess
import sys

def install_packages():
    """Установить необходимые пакеты"""
    packages = [
        'pyhive',
        'thrift',
        'sasl',
        'thrift-sasl'
    ]
    
    print("Установка зависимостей для работы с Hive...")
    
    for package in packages:
        try:
            subprocess.run([sys.executable, '-m', 'pip', 'install', package], 
                         check=True, capture_output=True)
            print(f"✓ Установлен: {package}")
        except subprocess.CalledProcessError:
            print(f"✗ Ошибка установки: {package}")

if __name__ == '__main__':
    install_packages()