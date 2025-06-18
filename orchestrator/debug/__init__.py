"""
Debug System for Orchestrator

This package provides a modular debug interface system that allows
creating custom debug tools for different modules in the orchestrator.
"""

__version__ = "1.0.0"

from .registry import DebugModuleRegistry

__all__ = ['DebugModuleRegistry'] 