import React, { useState } from 'react';

const UserTimelineViewer = ({ data }) => {
    const [selectedUser, setSelectedUser] = useState('');
    const [eventFilter, setEventFilter] = useState('all');
    const [expandedEvents, setExpandedEvents] = useState(new Set());

    // Debug logging
    console.log('[DEBUG] UserTimelineViewer received data:', data);
    console.log('[DEBUG] Data structure check:', {
        hasData: !!data,
        hasDataData: !!data?.data,
        hasUserTimelines: !!data?.data?.user_timelines,
        hasTimelineData: !!data?.data?.timeline_data,
        hasStructuredFormat: !!data?.data?.structured_format,
        hasTimelineAnalysis: !!data?.data?.structured_format?.timeline_analysis,
        dataKeys: data ? Object.keys(data) : [],
        dataDataKeys: data?.data ? Object.keys(data.data) : []
    });

    if (!data?.data?.user_timelines && !data?.data?.timeline_data && !data?.data?.structured_format?.timeline_analysis && !data?.data) {
        return (
            <div className="text-center py-8">
                <p className="text-gray-500 dark:text-gray-400">
                    No timeline data available. Run an analysis to see results.
                </p>
            </div>
        );
    }

    // Handle multiple data formats: unified pipeline, legacy API, and direct timeline API
    let timelineData;
    let userTimelines = {};
    
    if (data.data.structured_format?.timeline_analysis) {
        // Unified pipeline format: timeline data is in structured_format.timeline_analysis
        timelineData = data.data.structured_format.timeline_analysis;
        userTimelines = timelineData.user_timelines || {};
        console.log('[DEBUG] Using unified pipeline format. Timeline data:', timelineData);
        console.log('[DEBUG] User timelines keys:', Object.keys(userTimelines));
        console.log('[DEBUG] Sample user timeline:', Object.keys(userTimelines).slice(0, 1).map(userId => ({
            userId,
            data: userTimelines[userId]
        })));
    } else if (data.data.timeline_data && data.data.timeline_data.user_timelines) {
        // Direct timeline API format: timeline_data contains the TimelineData structure
        timelineData = data.data.timeline_data;
        userTimelines = timelineData.user_timelines || {};
    } else if (data.data.user_timelines && typeof data.data.user_timelines === 'object' && data.data.user_timelines.users) {
        // Legacy format: user_timelines contains the entire legacy result
        timelineData = data.data.user_timelines;
        // Convert legacy format to expected structure
        if (timelineData.users && timelineData.events) {
            timelineData.users.forEach(user => {
                userTimelines[user.id] = {
                    events: Object.values(timelineData.events[user.user_product_key] || {}).flat() || []
                };
            });
        }
    } else if (data.data.user_timelines) {
        // New format: user_timelines is a direct mapping
        timelineData = data.data;
        userTimelines = data.data.user_timelines || {};
    } else {
        // Fallback: no valid data
        return (
            <div className="text-center py-8">
                <p className="text-gray-500 dark:text-gray-400">
                    No timeline data available. Run an analysis to see results.
                </p>
            </div>
        );
    }
    
    const userIds = Object.keys(userTimelines);

    // Get events for selected user
    const selectedUserEvents = selectedUser && userTimelines[selectedUser] ? 
        userTimelines[selectedUser].events || [] : [];

    // Filter events based on event filter
    const filteredEvents = selectedUserEvents.filter(event => {
        if (eventFilter === 'all') return true;
        if (eventFilter === 'trial') return event.event_name?.toLowerCase().includes('trial');
        if (eventFilter === 'purchase') return event.event_name?.toLowerCase().includes('purchase');
        if (eventFilter === 'revenue') return event.revenue && event.revenue > 0;
        return true;
    });

    const formatCurrency = (value) => {
        if (value === null || value === undefined) return '$0.00';
        return `$${Number(value).toFixed(2)}`;
    };

    const formatDateTime = (timestamp) => {
        return new Date(timestamp).toLocaleString();
    };

    const toggleEventExpansion = (eventIndex) => {
        const newExpanded = new Set(expandedEvents);
        if (newExpanded.has(eventIndex)) {
            newExpanded.delete(eventIndex);
        } else {
            newExpanded.add(eventIndex);
        }
        setExpandedEvents(newExpanded);
    };

    const getEventTypeColor = (eventName) => {
        const name = eventName?.toLowerCase() || '';
        if (name.includes('trial')) return 'blue';
        if (name.includes('purchase')) return 'green';
        if (name.includes('revenue')) return 'purple';
        return 'gray';
    };

    const getEventTypeIcon = (eventName) => {
        const name = eventName?.toLowerCase() || '';
        if (name.includes('trial')) return '🔄';
        if (name.includes('purchase')) return '💰';
        if (name.includes('revenue')) return '💵';
        return '📊';
    };

    // Calculate user summary stats
    const userSummary = selectedUser && userTimelines[selectedUser] ? {
        totalEvents: selectedUserEvents.length,
        totalRevenue: selectedUserEvents.reduce((sum, event) => sum + (event.revenue || 0), 0),
        trialEvents: selectedUserEvents.filter(e => e.event_name?.toLowerCase().includes('trial')).length,
        purchaseEvents: selectedUserEvents.filter(e => e.event_name?.toLowerCase().includes('purchase')).length,
        firstEvent: selectedUserEvents.length > 0 ? selectedUserEvents[0].timestamp : null,
        lastEvent: selectedUserEvents.length > 0 ? selectedUserEvents[selectedUserEvents.length - 1].timestamp : null
    } : null;

    return (
        <div className="space-y-6">
            {/* User Selection and Filters */}
            <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                <div>
                    <label className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-2">
                        Select User
                    </label>
                    <select
                        value={selectedUser}
                        onChange={(e) => setSelectedUser(e.target.value)}
                        className="w-full px-3 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-blue-500 dark:bg-gray-700 dark:border-gray-600 dark:text-white"
                    >
                        <option value="">Choose a user...</option>
                        {userIds.slice(0, 100).map(userId => (
                            <option key={userId} value={userId}>
                                {userId.length > 30 ? userId.substring(0, 30) + '...' : userId}
                            </option>
                        ))}
                    </select>
                    <p className="text-xs text-gray-500 dark:text-gray-400 mt-1">
                        Showing first 100 users ({userIds.length} total)
                    </p>
                </div>

                <div>
                    <label className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-2">
                        Event Filter
                    </label>
                    <select
                        value={eventFilter}
                        onChange={(e) => setEventFilter(e.target.value)}
                        className="w-full px-3 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-blue-500 dark:bg-gray-700 dark:border-gray-600 dark:text-white"
                    >
                        <option value="all">All Events</option>
                        <option value="trial">Trial Events</option>
                        <option value="purchase">Purchase Events</option>
                        <option value="revenue">Revenue Events</option>
                    </select>
                </div>
            </div>

            {/* User Summary */}
            {userSummary && (
                <div className="bg-gradient-to-r from-indigo-50 to-purple-50 dark:from-indigo-900/20 dark:to-purple-900/20 rounded-lg p-6 border border-indigo-200 dark:border-indigo-800">
                    <h3 className="text-xl font-semibold text-indigo-900 dark:text-indigo-200 mb-4">
                        User Summary: {selectedUser}
                    </h3>
                    <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
                        <div className="text-center">
                            <div className="text-2xl font-bold text-indigo-600 dark:text-indigo-400">
                                {userSummary.totalEvents}
                            </div>
                            <div className="text-sm text-indigo-700 dark:text-indigo-300">
                                Total Events
                            </div>
                        </div>
                        <div className="text-center">
                            <div className="text-2xl font-bold text-green-600 dark:text-green-400">
                                {formatCurrency(userSummary.totalRevenue)}
                            </div>
                            <div className="text-sm text-green-700 dark:text-green-300">
                                Total Revenue
                            </div>
                        </div>
                        <div className="text-center">
                            <div className="text-2xl font-bold text-blue-600 dark:text-blue-400">
                                {userSummary.trialEvents}
                            </div>
                            <div className="text-sm text-blue-700 dark:text-blue-300">
                                Trial Events
                            </div>
                        </div>
                        <div className="text-center">
                            <div className="text-2xl font-bold text-purple-600 dark:text-purple-400">
                                {userSummary.purchaseEvents}
                            </div>
                            <div className="text-sm text-purple-700 dark:text-purple-300">
                                Purchase Events
                            </div>
                        </div>
                    </div>
                    {userSummary.firstEvent && userSummary.lastEvent && (
                        <div className="mt-4 pt-4 border-t border-indigo-200 dark:border-indigo-700 text-sm text-indigo-700 dark:text-indigo-300">
                            <strong>Activity Period:</strong> {formatDateTime(userSummary.firstEvent)} → {formatDateTime(userSummary.lastEvent)}
                        </div>
                    )}
                </div>
            )}

            {/* Event Timeline */}
            {selectedUser && (
                <div>
                    <div className="flex justify-between items-center mb-4">
                        <h3 className="text-xl font-semibold text-gray-900 dark:text-white">
                            Event Timeline
                        </h3>
                        <div className="text-sm text-gray-600 dark:text-gray-400">
                            Showing {filteredEvents.length} of {selectedUserEvents.length} events
                        </div>
                    </div>

                    {filteredEvents.length > 0 ? (
                        <div className="space-y-3">
                            {filteredEvents.map((event, index) => {
                                const isExpanded = expandedEvents.has(index);
                                const eventColor = getEventTypeColor(event.event_name);
                                const eventIcon = getEventTypeIcon(event.event_name);

                                const colorClasses = {
                                    blue: 'border-blue-200 bg-blue-50 dark:border-blue-800 dark:bg-blue-900/20',
                                    green: 'border-green-200 bg-green-50 dark:border-green-800 dark:bg-green-900/20',
                                    purple: 'border-purple-200 bg-purple-50 dark:border-purple-800 dark:bg-purple-900/20',
                                    gray: 'border-gray-200 bg-gray-50 dark:border-gray-600 dark:bg-gray-700'
                                };

                                return (
                                    <div
                                        key={index}
                                        className={`border rounded-lg p-4 ${colorClasses[eventColor]} transition-all duration-200`}
                                    >
                                        <div 
                                            className="flex items-center justify-between cursor-pointer"
                                            onClick={() => toggleEventExpansion(index)}
                                        >
                                            <div className="flex items-center space-x-3">
                                                <span className="text-lg">{eventIcon}</span>
                                                <div>
                                                    <div className="font-medium text-gray-900 dark:text-white">
                                                        {event.event_name || 'Unknown Event'}
                                                    </div>
                                                    <div className="text-sm text-gray-600 dark:text-gray-400">
                                                        {formatDateTime(event.timestamp)}
                                                    </div>
                                                </div>
                                            </div>
                                            <div className="flex items-center space-x-3">
                                                {event.revenue && event.revenue > 0 && (
                                                    <span className="text-sm font-semibold text-green-600 dark:text-green-400">
                                                        {formatCurrency(event.revenue)}
                                                    </span>
                                                )}
                                                <button className="text-gray-400 hover:text-gray-600 dark:hover:text-gray-300">
                                                    {isExpanded ? '▼' : '▶'}
                                                </button>
                                            </div>
                                        </div>

                                        {isExpanded && (
                                            <div className="mt-4 pt-4 border-t border-gray-200 dark:border-gray-600">
                                                <div className="grid grid-cols-1 md:grid-cols-2 gap-4 text-sm">
                                                    {event.product_id && (
                                                        <div>
                                                            <span className="font-medium text-gray-700 dark:text-gray-300">Product ID:</span>
                                                            <span className="ml-2 text-gray-600 dark:text-gray-400">{event.product_id}</span>
                                                        </div>
                                                    )}
                                                    {event.revenue && (
                                                        <div>
                                                            <span className="font-medium text-gray-700 dark:text-gray-300">Revenue:</span>
                                                            <span className="ml-2 text-green-600 dark:text-green-400">{formatCurrency(event.revenue)}</span>
                                                        </div>
                                                    )}
                                                    {event.properties && Object.keys(event.properties).length > 0 && (
                                                        <div className="md:col-span-2">
                                                            <span className="font-medium text-gray-700 dark:text-gray-300">Properties:</span>
                                                            <div className="mt-2 bg-gray-100 dark:bg-gray-800 rounded p-2 text-xs">
                                                                <pre className="whitespace-pre-wrap">
                                                                    {JSON.stringify(event.properties, null, 2)}
                                                                </pre>
                                                            </div>
                                                        </div>
                                                    )}
                                                </div>
                                            </div>
                                        )}
                                    </div>
                                );
                            })}
                        </div>
                    ) : selectedUser ? (
                        <div className="text-center py-8">
                            <p className="text-gray-500 dark:text-gray-400">
                                No events found for the selected filter.
                            </p>
                        </div>
                    ) : null}
                </div>
            )}

            {!selectedUser && (
                <div className="text-center py-12">
                    <div className="text-6xl mb-4">👤</div>
                    <h3 className="text-lg font-medium text-gray-900 dark:text-white mb-2">
                        Select a User
                    </h3>
                    <p className="text-gray-500 dark:text-gray-400">
                        Choose a user from the dropdown above to view their detailed event timeline.
                    </p>
                </div>
            )}
        </div>
    );
};

export default UserTimelineViewer; 