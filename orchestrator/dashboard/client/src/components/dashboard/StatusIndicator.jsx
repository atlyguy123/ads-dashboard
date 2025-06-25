import React from 'react';
import { CheckCircle, RefreshCw, AlertTriangle, Info } from 'lucide-react';

const StatusIndicator = ({ 
  status = 'idle', // 'idle', 'loading', 'success', 'error', 'info'
  message,
  size = 'sm'
}) => {
  const sizeClasses = {
    sm: 'h-3 w-3',
    md: 'h-4 w-4',
    lg: 'h-5 w-5'
  };

  const getStatusConfig = () => {
    switch (status) {
      case 'loading':
        return {
          icon: RefreshCw,
          color: 'text-blue-600 dark:text-blue-400',
          bgColor: 'bg-blue-100 dark:bg-blue-900/30',
          borderColor: 'border-blue-200 dark:border-blue-800',
          animate: 'animate-spin'
        };
      case 'success':
        return {
          icon: CheckCircle,
          color: 'text-green-600 dark:text-green-400',
          bgColor: 'bg-green-100 dark:bg-green-900/30',
          borderColor: 'border-green-200 dark:border-green-800',
          animate: ''
        };
      case 'error':
        return {
          icon: AlertTriangle,
          color: 'text-red-600 dark:text-red-400',
          bgColor: 'bg-red-100 dark:bg-red-900/30',
          borderColor: 'border-red-200 dark:border-red-800',
          animate: ''
        };
      case 'info':
        return {
          icon: Info,
          color: 'text-blue-600 dark:text-blue-400',
          bgColor: 'bg-blue-100 dark:bg-blue-900/30',
          borderColor: 'border-blue-200 dark:border-blue-800',
          animate: ''
        };
      default: // idle
        return {
          icon: null,
          color: 'text-gray-400 dark:text-gray-500',
          bgColor: 'bg-gray-100 dark:bg-gray-700',
          borderColor: 'border-gray-200 dark:border-gray-600',
          animate: ''
        };
    }
  };

  const config = getStatusConfig();
  const Icon = config.icon;

  return (
    <div className={`inline-flex items-center space-x-2 px-3 py-1.5 rounded-lg text-sm border ${config.bgColor} ${config.borderColor} ${config.color}`}>
      {Icon && (
        <Icon className={`${sizeClasses[size]} ${config.animate}`} />
      )}
      {status === 'idle' && (
        <div className={`${sizeClasses[size]} rounded-full bg-gray-300 dark:bg-gray-600`} />
      )}
      {message && (
        <span className="font-medium">
          {message}
        </span>
      )}
    </div>
  );
};

export default StatusIndicator; 