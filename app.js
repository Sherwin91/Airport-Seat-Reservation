/* ==========================================================================
   Airport Seat Reservation Simulator - Virtual OS Lab
   Producer–Consumer & Synchronization State Machine Engine
   ========================================================================== */

document.addEventListener('DOMContentLoaded', () => {
  // State Initialization
  const STATE = {
    passengers: [
      { id: 1, name: 'Passenger 1', seat: 5, status: 'WAITING' },
      { id: 2, name: 'Passenger 2', seat: 3, status: 'WAITING' },
      { id: 3, name: 'Passenger 3', seat: 5, status: 'WAITING' }, // Duplicate seat target
      { id: 4, name: 'Passenger 4', seat: 8, status: 'WAITING' },
      { id: 5, name: 'Passenger 5', seat: 2, status: 'WAITING' },
      { id: 6, name: 'Passenger 6', seat: 5, status: 'WAITING' }, // Triple duplicate seat target
      { id: 7, name: 'Passenger 7', seat: 1, status: 'WAITING' },
      { id: 8, name: 'Passenger 8', seat: 3, status: 'WAITING' }, // Duplicate seat 3
      { id: 9, name: 'Passenger 9', seat: 7, status: 'WAITING' },
      { id: 10, name: 'Passenger 10', seat: 4, status: 'WAITING' }
    ],
    bufferCapacity: 5,
    buffer: [], // Max 5 items: { passengerId, seat, reqId }
    semaphores: {
      empty: 5,
      full: 0
    },
    mutexes: {
      buffer_mutex: { locked: false, owner: null, waiting: [] },
      seat_mutex: { locked: false, owner: null, waiting: [] }
    },
    agents: [
      { id: 1, name: 'Agent 1', status: 'IDLE', currentReq: null },
      { id: 2, name: 'Agent 2', status: 'IDLE', currentReq: null },
      { id: 3, name: 'Agent 3', status: 'IDLE', currentReq: null }
    ],
    seats: Array.from({ length: 10 }, (_, i) => ({
      id: i + 1,
      status: 'AVAILABLE', // AVAILABLE, PROCESSING, RESERVED
      assignedTo: null,
      lockedBy: null
    })),
    stats: {
      totalPassengers: 10,
      requestsGenerated: 0,
      requestsProcessed: 0,
      successfulReservations: 0,
      failedReservations: 0,
      availableSeats: 10,
      reservedSeats: 0,
      bufferUsage: '0 / 5',
      raceConditionsPrevented: 0,
      duplicateBookings: 0
    },
    stepQueue: [],
    currentStepIndex: 0,
    isRunningAuto: false,
    autoTimer: null,
    speedMs: 800,
    syncEnabled: true,
    activeWfNode: null
  };

  // DOM Elements
  const dom = {
    // Control Buttons
    btnAutoRun: document.getElementById('btn-auto-run'),
    btnPause: document.getElementById('btn-pause'),
    btnNextStep: document.getElementById('btn-next-step'),
    btnReset: document.getElementById('btn-reset'),
    btnRaceDemo: document.getElementById('btn-race-demo'),
    syncToggleBtn: document.getElementById('sync-toggle-btn'),
    speedBtns: document.querySelectorAll('.speed-btn'),
    tabBtns: document.querySelectorAll('.tab-btn'),
    tabContents: document.querySelectorAll('.tab-content'),
    
    // Step Banner
    stepBadge: document.getElementById('step-badge'),
    stepDesc: document.getElementById('step-desc'),
    
    // Containers
    passengerList: document.getElementById('passenger-list'),
    bufferSlots: document.getElementById('buffer-slots'),
    bufferAlert: document.getElementById('buffer-alert'),
    bufferUsageBar: document.getElementById('buffer-usage-bar'),
    bufferUsageText: document.getElementById('buffer-usage-text'),
    agentList: document.getElementById('agent-list'),
    seatGrid: document.getElementById('seat-grid'),
    eventLogTerminal: document.getElementById('event-log-terminal'),
    
    // Sync Displays
    semEmptyVal: document.getElementById('sem-empty-val'),
    semFullVal: document.getElementById('sem-full-val'),
    bufferMutexTag: document.getElementById('buffer-mutex-tag'),
    bufferMutexOwner: document.getElementById('buffer-mutex-owner'),
    seatMutexTag: document.getElementById('seat-mutex-tag'),
    seatMutexOwner: document.getElementById('seat-mutex-owner'),
    seatMutexWaiting: document.getElementById('seat-mutex-waiting'),
    
    // Stats
    statGenerated: document.getElementById('stat-generated'),
    statProcessed: document.getElementById('stat-processed'),
    statSuccessful: document.getElementById('stat-successful'),
    statFailed: document.getElementById('stat-failed'),
    statAvailableSeats: document.getElementById('stat-available-seats'),
    statReservedSeats: document.getElementById('stat-reserved-seats'),
    statBufferUsage: document.getElementById('stat-buffer-usage'),
    statRacePrevented: document.getElementById('stat-race-prevented'),
    statDuplicates: document.getElementById('stat-duplicates'),
    
    // Race Demo Elements
    raceUnsyncLog: document.getElementById('race-unsync-log'),
    raceSyncLog: document.getElementById('race-sync-log'),
    btnRunRaceUnsync: document.getElementById('btn-run-race-unsync'),
    btnRunRaceSync: document.getElementById('btn-run-race-sync'),
    completionPanel: document.getElementById('completion-panel')
  };

  // Pre-build simulation step queue
  function generateStepQueue() {
    const queue = [];
    let reqCounter = 1;
    let passengerIdx = 0;
    
    // We simulate interleaved producers and consumers
    while (passengerIdx < STATE.passengers.length) {
      if (passengerIdx < STATE.passengers.length) {
        const p = STATE.passengers[passengerIdx];
        const reqId = reqCounter++;
        
        // Producer Steps
        queue.push({
          type: 'PRODUCE_START',
          passenger: p,
          reqId: reqId,
          desc: `STEP ${queue.length + 1}: ${p.name} creates booking request for Seat ${p.seat}.`
        });
        
        queue.push({
          type: 'PRODUCE_SEM_WAIT',
          passenger: p,
          reqId: reqId,
          desc: `STEP ${queue.length + 1}: ${p.name} executes wait(empty). Checking available buffer slots.`
        });

        queue.push({
          type: 'PRODUCE_LOCK_MUTEX',
          passenger: p,
          reqId: reqId,
          desc: `STEP ${queue.length + 1}: ${p.name} acquires buffer_mutex.`
        });

        queue.push({
          type: 'PRODUCE_INSERT_BUFFER',
          passenger: p,
          reqId: reqId,
          desc: `STEP ${queue.length + 1}: Request (${p.name} → Seat ${p.seat}) inserted into Bounded Buffer.`
        });

        queue.push({
          type: 'PRODUCE_UNLOCK_MUTEX',
          passenger: p,
          reqId: reqId,
          desc: `STEP ${queue.length + 1}: ${p.name} releases buffer_mutex and signals signal(full).`
        });

        passengerIdx++;
      }

      // Add corresponding consumer steps for active agents
      // Fill the bounded buffer first, then consume one request per producer.
      // This deterministic schedule makes BUFFER FULL observable and guarantees
      // every generated request is eventually processed.
      if (passengerIdx >= 5 || passengerIdx === STATE.passengers.length) {
        const agentId = (queue.filter(q => q.type === 'CONSUME_START').length % 3) + 1;
        const agent = STATE.agents[agentId - 1];

        queue.push({
          type: 'CONSUME_START',
          agent: agent,
          desc: `STEP ${queue.length + 1}: ${agent.name} executes wait(full) to fetch request from buffer.`
        });

        queue.push({
          type: 'CONSUME_LOCK_BUFFER_MUTEX',
          agent: agent,
          desc: `STEP ${queue.length + 1}: ${agent.name} acquires buffer_mutex.`
        });

        queue.push({
          type: 'CONSUME_REMOVE_BUFFER',
          agent: agent,
          desc: `STEP ${queue.length + 1}: ${agent.name} removes request from Bounded Buffer.`
        });

        queue.push({
          type: 'CONSUME_UNLOCK_BUFFER_MUTEX',
          agent: agent,
          desc: `STEP ${queue.length + 1}: ${agent.name} releases buffer_mutex and signals signal(empty).`
        });

        queue.push({
          type: 'CONSUME_LOCK_SEAT_MUTEX',
          agent: agent,
          desc: `STEP ${queue.length + 1}: ${agent.name} attempts to acquire seat_mutex to enter Critical Section.`
        });

        queue.push({
          type: 'CONSUME_CHECK_RESERVE_SEAT',
          agent: agent,
          desc: `STEP ${queue.length + 1}: ${agent.name} checks seat status in critical section and reserves/rejects.`
        });

        queue.push({
          type: 'CONSUME_UNLOCK_SEAT_MUTEX',
          agent: agent,
          desc: `STEP ${queue.length + 1}: ${agent.name} unlocks seat_mutex and completes transaction.`
        });
      }

    }

    // Drain the four requests produced before the first consumer was scheduled.
    const scheduledConsumers = queue.filter(step => step.type === 'CONSUME_START').length;
    for (let i = scheduledConsumers; i < STATE.passengers.length; i++) {
      const agent = STATE.agents[i % STATE.agents.length];
      queue.push({
        type: 'CONSUME_START',
        agent,
        desc: `STEP ${queue.length + 1}: ${agent.name} executes wait(full) to fetch the remaining request.`
      });
      queue.push({
        type: 'CONSUME_LOCK_BUFFER_MUTEX',
        agent,
        desc: `STEP ${queue.length + 1}: ${agent.name} acquires buffer_mutex.`
      });
      queue.push({
        type: 'CONSUME_REMOVE_BUFFER',
        agent,
        desc: `STEP ${queue.length + 1}: ${agent.name} removes request from Bounded Buffer.`
      });
      queue.push({
        type: 'CONSUME_UNLOCK_BUFFER_MUTEX',
        agent,
        desc: `STEP ${queue.length + 1}: ${agent.name} releases buffer_mutex and signals signal(empty).`
      });
      queue.push({
        type: 'CONSUME_LOCK_SEAT_MUTEX',
        agent,
        desc: `STEP ${queue.length + 1}: ${agent.name} attempts to acquire seat_mutex.`
      });
      queue.push({
        type: 'CONSUME_CHECK_RESERVE_SEAT',
        agent,
        desc: `STEP ${queue.length + 1}: ${agent.name} checks seat status and reserves/rejects.`
      });
      queue.push({
        type: 'CONSUME_UNLOCK_SEAT_MUTEX',
        agent,
        desc: `STEP ${queue.length + 1}: ${agent.name} unlocks seat_mutex and completes transaction.`
      });
    }

    return queue;
  }

  // Render Functions
  function renderPassengers() {
    dom.passengerList.innerHTML = STATE.passengers.map(p => {
      let statusClass = 'status-waiting';
      if (p.status === 'PRODUCING') statusClass = 'status-producing';
      if (p.status === 'QUEUED') statusClass = 'status-queued';
      if (p.status === 'SERVED') statusClass = 'status-served';
      if (p.status === 'REJECTED') statusClass = 'status-rejected';

      const isActive = p.status === 'PRODUCING';
      const isBlocked = p.status === 'BLOCKED';

      return `
        <div class="passenger-card ${isActive ? 'active-producer' : ''} ${isBlocked ? 'blocked-producer' : ''}">
          <div class="passenger-info">
            <div class="passenger-avatar">P${p.id}</div>
            <div>
              <div class="passenger-name">${p.name}</div>
              <span class="seat-request-badge">Target: Seat ${p.seat}</span>
            </div>
          </div>
          <span class="passenger-status ${statusClass}">${p.status}</span>
        </div>
      `;
    }).join('');
  }

  function renderBuffer() {
    const slotsHTML = [];
    for (let i = 0; i < STATE.bufferCapacity; i++) {
      const item = STATE.buffer[i];
      if (item) {
        slotsHTML.push(`
          <div class="buffer-slot occupied">
            <span class="buffer-slot-index">Slot [${i}]</span>
            <div class="buffer-slot-content">
              <div class="request-packet">
                <span style="font-weight:700; color:var(--accent-cyan);">P${item.passengerId}</span>
                <span>➔</span>
                <span class="seat-request-badge">Seat ${item.seat}</span>
              </div>
            </div>
          </div>
        `);
      } else {
        slotsHTML.push(`
          <div class="buffer-slot">
            <span class="buffer-slot-index">Slot [${i}]</span>
            <span style="font-size:0.8rem; color:var(--text-muted); font-style:italic;">[ Empty Slot ]</span>
          </div>
        `);
      }
    }
    dom.bufferSlots.innerHTML = slotsHTML.join('');

    const count = STATE.buffer.length;
    dom.bufferUsageText.textContent = `Buffer: ${count} / ${STATE.bufferCapacity}`;
    const pct = (count / STATE.bufferCapacity) * 100;
    dom.bufferUsageBar.style.width = `${pct}%`;

    if (count === STATE.bufferCapacity) {
      dom.bufferAlert.className = 'buffer-alert-banner alert-full';
      dom.bufferAlert.innerHTML = '⚠️ BUFFER FULL — Producers WAIT';
      dom.bufferUsageBar.className = 'buffer-usage-bar full';
    } else if (count === 0) {
      dom.bufferAlert.className = 'buffer-alert-banner alert-empty';
      dom.bufferAlert.innerHTML = 'ℹ️ BUFFER EMPTY — Consumers WAIT';
      dom.bufferUsageBar.className = 'buffer-usage-bar';
    } else {
      dom.bufferAlert.className = 'buffer-alert-banner alert-normal';
      dom.bufferAlert.innerHTML = '✅ NORMAL OPERATION — Slots Available';
      dom.bufferUsageBar.className = 'buffer-usage-bar';
    }
  }

  function renderAgents() {
    dom.agentList.innerHTML = STATE.agents.map(a => {
      let isCritical = a.status === 'CRITICAL_SECTION';
      let isWaiting = a.status === 'WAITING_MUTEX';
      let isActive = a.status === 'FETCHING' || a.status === 'RESERVING';

      return `
        <div class="agent-card ${isCritical ? 'critical-section' : ''} ${isWaiting ? 'waiting-mutex' : ''} ${isActive ? 'active-consumer' : ''}">
          <div class="agent-header">
            <div class="agent-title">
              <div class="agent-badge-icon">A${a.id}</div>
              <span>${a.name}</span>
            </div>
            <span class="passenger-status ${isCritical ? 'status-producing' : isWaiting ? 'status-queued' : 'status-waiting'}">${a.status}</span>
          </div>
          <div class="agent-processing-box">
            <span>Processing:</span>
            <span style="font-weight:700; color:${a.currentReq ? 'var(--accent-cyan)' : 'var(--text-muted)'}">
              ${a.currentReq ? `P${a.currentReq.passengerId} ➔ Seat ${a.currentReq.seat}` : 'None (Idle)'}
            </span>
          </div>
        </div>
      `;
    }).join('');
  }

  function renderSyncPanels() {
    dom.semEmptyVal.textContent = STATE.semaphores.empty;
    dom.semFullVal.textContent = STATE.semaphores.full;

    // Buffer Mutex
    const bm = STATE.mutexes.buffer_mutex;
    if (bm.locked) {
      dom.bufferMutexTag.className = 'mutex-state-tag mutex-locked';
      dom.bufferMutexTag.innerHTML = '🔒 LOCKED';
      dom.bufferMutexOwner.textContent = `Locked by: ${bm.owner}`;
    } else {
      dom.bufferMutexTag.className = 'mutex-state-tag mutex-unlocked';
      dom.bufferMutexTag.innerHTML = '🔓 UNLOCKED';
      dom.bufferMutexOwner.textContent = 'Available';
    }

    // Seat Mutex
    const sm = STATE.mutexes.seat_mutex;
    if (sm.locked) {
      dom.seatMutexTag.className = 'mutex-state-tag mutex-locked';
      dom.seatMutexTag.innerHTML = '🔒 LOCKED';
      dom.seatMutexOwner.textContent = `Locked by: ${sm.owner}`;
      dom.seatMutexWaiting.textContent = sm.waiting.length > 0 ? `Waiting: ${sm.waiting.join(', ')}` : 'Waiting: None';
    } else {
      dom.seatMutexTag.className = 'mutex-state-tag mutex-unlocked';
      dom.seatMutexTag.innerHTML = '🔓 UNLOCKED';
      dom.seatMutexOwner.textContent = 'Available';
      dom.seatMutexWaiting.textContent = 'Waiting: None';
    }
  }

  function renderSeatGrid() {
    dom.seatGrid.innerHTML = STATE.seats.map(s => {
      let stateClass = 'state-available';
      let statusText = 'AVAILABLE';

      if (s.status === 'PROCESSING') {
        stateClass = 'state-processing';
        statusText = 'LOCKED / PROC';
      } else if (s.status === 'RESERVED') {
        stateClass = 'state-reserved';
        statusText = 'RESERVED';
      } else if (s.status === 'CONFLICT') {
        stateClass = 'state-conflict';
        statusText = '❌ DUPLICATE!';
      }

      return `
        <div class="seat-card ${stateClass}">
          <div class="seat-number">Seat ${s.id}</div>
          <div class="seat-status-text">${statusText}</div>
          <div class="seat-assigned">
            ${s.assignedTo ? `Passenger P${s.assignedTo}` : s.lockedBy ? `Agent A${s.lockedBy}` : 'Unassigned'}
          </div>
        </div>
      `;
    }).join('');
  }

  function renderStats() {
    dom.statGenerated.textContent = STATE.stats.requestsGenerated;
    dom.statProcessed.textContent = STATE.stats.requestsProcessed;
    dom.statSuccessful.textContent = STATE.stats.successfulReservations;
    dom.statFailed.textContent = STATE.stats.failedReservations;
    dom.statAvailableSeats.textContent = STATE.stats.availableSeats;
    dom.statReservedSeats.textContent = STATE.stats.reservedSeats;
    dom.statBufferUsage.textContent = `${STATE.buffer.length} / ${STATE.bufferCapacity}`;
    dom.statRacePrevented.textContent = STATE.stats.raceConditionsPrevented;
    dom.statDuplicates.textContent = STATE.stats.duplicateBookings;
  }

  function addLog(message, type = 'normal') {
    const timeStr = new Date().toLocaleTimeString();
    const entry = document.createElement('div');
    entry.className = 'log-entry';
    entry.innerHTML = `
      <span class="log-time">[${timeStr}]</span>
      <span class="log-msg type-${type}">${message}</span>
    `;
    dom.eventLogTerminal.appendChild(entry);
    dom.eventLogTerminal.scrollTop = dom.eventLogTerminal.scrollHeight;
  }

  function highlightWorkflowNode(nodeId) {
    document.querySelectorAll('.wf-node').forEach(n => n.classList.remove('active-wf'));
    if (nodeId) {
      const el = document.getElementById(`wf-${nodeId}`);
      if (el) el.classList.add('active-wf');
    }
  }

  function highlightCodeLine(lineNum) {
    document.querySelectorAll('.code-line').forEach(l => l.classList.remove('active-code-line'));
    if (lineNum) {
      const lineEl = document.getElementById(`code-line-${lineNum}`);
      if (lineEl) lineEl.classList.add('active-code-line');
    }
  }

  // Core Simulation Step Executor
  function executeNextStep() {
    if (STATE.currentStepIndex >= STATE.stepQueue.length) {
      STATE.isRunningAuto = false;
      clearInterval(STATE.autoTimer);
      dom.btnAutoRun.disabled = false;
      dom.btnPause.disabled = true;
      dom.stepBadge.textContent = 'DONE';
      dom.stepDesc.textContent = 'All producer and consumer operations completed. Reset to run the experiment again.';
      dom.completionPanel.hidden = false;
      addLog('SIMULATION COMPLETED — all requests processed.', 'success');
      return;
    }

    const step = STATE.stepQueue[STATE.currentStepIndex];
    if (!step) return;

    STATE.currentStepIndex++;
    dom.stepBadge.textContent = `STEP ${STATE.currentStepIndex}`;
    dom.stepDesc.textContent = step.desc;

    switch (step.type) {
      case 'PRODUCE_START':
        step.passenger.status = 'PRODUCING';
        STATE.stats.requestsGenerated++;
        addLog(`${step.passenger.name} generated request for Seat ${step.passenger.seat}`, 'producer');
        highlightWorkflowNode('produce');
        highlightCodeLine(2);
        break;

      case 'PRODUCE_SEM_WAIT':
        highlightWorkflowNode('empty-sem');
        highlightCodeLine(3);
        if (STATE.semaphores.empty <= 0) {
          step.passenger.status = 'BLOCKED';
          addLog(`BUFFER FULL! ${step.passenger.name} BLOCKED on wait(empty)`, 'mutex');
        } else {
          STATE.semaphores.empty--;
          addLog(`Producer wait(empty) passed. Empty slots: ${STATE.semaphores.empty}`, 'producer');
        }
        break;

      case 'PRODUCE_LOCK_MUTEX':
        STATE.mutexes.buffer_mutex.locked = true;
        STATE.mutexes.buffer_mutex.owner = step.passenger.name;
        highlightWorkflowNode('buf-mutex');
        highlightCodeLine(4);
        addLog(`${step.passenger.name} locked buffer_mutex`, 'mutex');
        break;

      case 'PRODUCE_INSERT_BUFFER':
        if (STATE.buffer.length < STATE.bufferCapacity) {
          STATE.buffer.push({
            passengerId: step.passenger.id,
            seat: step.passenger.seat,
            reqId: step.reqId
          });
          step.passenger.status = 'QUEUED';
          addLog(`Request (P${step.passenger.id} ➔ Seat ${step.passenger.seat}) added to buffer`, 'success');
        }
        highlightWorkflowNode('buffer');
        highlightCodeLine(5);
        break;

      case 'PRODUCE_UNLOCK_MUTEX':
        STATE.mutexes.buffer_mutex.locked = false;
        STATE.mutexes.buffer_mutex.owner = null;
        STATE.semaphores.full++;
        highlightWorkflowNode('full-sem');
        highlightCodeLine(6);
        addLog(`Producer unlocked buffer_mutex and signaled signal(full). Full: ${STATE.semaphores.full}`, 'mutex');
        break;

      case 'CONSUME_START':
        step.agent.status = 'FETCHING';
        highlightWorkflowNode('agents');
        highlightCodeLine(10);
        if (STATE.semaphores.full <= 0) {
          addLog(`BUFFER EMPTY! ${step.agent.name} BLOCKED on wait(full)`, 'mutex');
        } else {
          STATE.semaphores.full--;
          addLog(`${step.agent.name} executed wait(full). Full slots: ${STATE.semaphores.full}`, 'consumer');
        }
        break;

      case 'CONSUME_LOCK_BUFFER_MUTEX':
        STATE.mutexes.buffer_mutex.locked = true;
        STATE.mutexes.buffer_mutex.owner = step.agent.name;
        highlightWorkflowNode('buf-mutex');
        highlightCodeLine(11);
        addLog(`${step.agent.name} acquired buffer_mutex`, 'mutex');
        break;

      case 'CONSUME_REMOVE_BUFFER':
        if (STATE.buffer.length > 0) {
          const req = STATE.buffer.shift();
          step.agent.currentReq = req;
          addLog(`${step.agent.name} consumed request (P${req.passengerId} ➔ Seat ${req.seat}) from buffer`, 'consumer');
        }
        highlightWorkflowNode('buffer');
        highlightCodeLine(12);
        break;

      case 'CONSUME_UNLOCK_BUFFER_MUTEX':
        STATE.mutexes.buffer_mutex.locked = false;
        STATE.mutexes.buffer_mutex.owner = null;
        STATE.semaphores.empty++;
        highlightWorkflowNode('empty-sem');
        highlightCodeLine(13);
        addLog(`${step.agent.name} released buffer_mutex & signaled signal(empty). Empty: ${STATE.semaphores.empty}`, 'mutex');
        break;

      case 'CONSUME_LOCK_SEAT_MUTEX':
        highlightWorkflowNode('seat-mutex');
        highlightCodeLine(16);
        if (STATE.syncEnabled) {
          if (STATE.mutexes.seat_mutex.locked) {
            step.agent.status = 'WAITING_MUTEX';
            STATE.mutexes.seat_mutex.waiting.push(step.agent.name);
            addLog(`${step.agent.name} WAITING for seat_mutex...`, 'mutex');
          } else {
            STATE.mutexes.seat_mutex.locked = true;
            STATE.mutexes.seat_mutex.owner = step.agent.name;
            step.agent.status = 'CRITICAL_SECTION';
            addLog(`${step.agent.name} acquired seat_mutex 🔒 (Entering Critical Section)`, 'mutex');
          }
        } else {
          // Unsynchronized race condition mode!
          step.agent.status = 'CRITICAL_SECTION';
          addLog(`⚠️ [UNSYNC MODE] ${step.agent.name} entered seat memory WITHOUT MUTEX!`, 'error');
        }
        break;

      case 'CONSUME_CHECK_RESERVE_SEAT':
        highlightWorkflowNode('check-seat');
        highlightCodeLine(17);
        if (step.agent.currentReq) {
          const req = step.agent.currentReq;
          const targetSeat = STATE.seats.find(s => s.id === req.seat);
          const passenger = STATE.passengers.find(p => p.id === req.passengerId);

          STATE.stats.requestsProcessed++;

          if (targetSeat) {
            if (targetSeat.status === 'AVAILABLE') {
              targetSeat.status = 'RESERVED';
              targetSeat.assignedTo = req.passengerId;
              if (passenger) passenger.status = 'SERVED';
              STATE.stats.successfulReservations++;
              STATE.stats.reservedSeats++;
              STATE.stats.availableSeats--;
              addLog(`✅ Seat ${req.seat} RESERVED for ${passenger ? passenger.name : `P${req.passengerId}`}`, 'success');
              highlightWorkflowNode('reserve');
            } else if (targetSeat.status === 'RESERVED') {
              if (passenger) passenger.status = 'REJECTED';
              STATE.stats.failedReservations++;
              STATE.stats.raceConditionsPrevented++;
              addLog(`❌ Seat ${req.seat} ALREADY RESERVED! ${step.agent.name} rejected ${passenger ? passenger.name : `P${req.passengerId}`} request`, 'error');
              highlightWorkflowNode('reject');
            }
          }
        }
        break;

      case 'CONSUME_UNLOCK_SEAT_MUTEX':
        highlightWorkflowNode('unlock-mutex');
        highlightCodeLine(21);
        if (STATE.syncEnabled && STATE.mutexes.seat_mutex.locked && STATE.mutexes.seat_mutex.owner === step.agent.name) {
          STATE.mutexes.seat_mutex.locked = false;
          STATE.mutexes.seat_mutex.owner = null;
          addLog(`${step.agent.name} released seat_mutex 🔓`, 'mutex');
        }
        step.agent.status = 'IDLE';
        step.agent.currentReq = null;
        highlightWorkflowNode('update-db');
        break;
    }

    // Update all UI elements
    renderPassengers();
    renderBuffer();
    renderAgents();
    renderSyncPanels();
    renderSeatGrid();
    renderStats();
  }

  // Interactive Race Condition Demo
  function runRaceDemoUnsynchronized() {
    dom.raceUnsyncLog.innerHTML = `
      <div class="timeline-step"><strong>[T=0ms]</strong> Passenger 3 & Passenger 6 both request Seat 5 simultaneously.</div>
      <div class="timeline-step"><strong>[T=5ms]</strong> Agent 1 fetches Passenger 3 request (Seat 5).</div>
      <div class="timeline-step"><strong>[T=6ms]</strong> Agent 2 fetches Passenger 6 request (Seat 5).</div>
      <div class="timeline-step" style="border-color:var(--accent-amber);"><strong>[T=10ms]</strong> Agent 1 checks Seat 5 state ➔ <span style="color:var(--accent-emerald);">AVAILABLE</span></div>
      <div class="timeline-step" style="border-color:var(--accent-amber);"><strong>[T=11ms]</strong> Agent 2 checks Seat 5 state (Without Mutex Lock!) ➔ <span style="color:var(--accent-emerald);">AVAILABLE</span></div>
      <div class="timeline-step" style="border-color:var(--accent-rose);"><strong>[T=15ms]</strong> Agent 1 writes Seat 5 ➔ RESERVED (Passenger 3).</div>
      <div class="timeline-step" style="border-color:var(--accent-rose);"><strong>[T=16ms]</strong> Agent 2 writes Seat 5 ➔ RESERVED (Passenger 6) <span style="color:var(--accent-rose); font-weight:700;">[OVERWRITING!]</span></div>
      <div class="timeline-step" style="background:rgba(244,63,94,0.15); border-color:var(--accent-rose);">
        <strong style="color:var(--accent-rose);">❌ RESULT: RACE CONDITION / DUPLICATE BOOKING!</strong><br>
        Both Agent 1 and Agent 2 reported successful booking for Seat 5. Passenger 6 overwrote Passenger 3.
      </div>
    `;

    STATE.stats.duplicateBookings++;
    renderStats();
  }

  function runRaceDemoSynchronized() {
    dom.raceSyncLog.innerHTML = `
      <div class="timeline-step"><strong>[T=0ms]</strong> Passenger 3 & Passenger 6 both request Seat 5 simultaneously.</div>
      <div class="timeline-step"><strong>[T=5ms]</strong> Agent 1 fetches Passenger 3 request. Agent 2 fetches Passenger 6 request.</div>
      <div class="timeline-step" style="border-color:var(--accent-cyan);"><strong>[T=10ms]</strong> Agent 1 acquires <code>seat_mutex</code> 🔒 (Locked).</div>
      <div class="timeline-step" style="border-color:var(--accent-amber);"><strong>[T=11ms]</strong> Agent 2 attempts <code>lock(seat_mutex)</code> ➔ <span style="color:var(--accent-amber);">BLOCKED / WAITING</span>.</div>
      <div class="timeline-step" style="border-color:var(--accent-emerald);"><strong>[T=15ms]</strong> Agent 1 checks Seat 5 (AVAILABLE) ➔ Reserves for Passenger 3.</div>
      <div class="timeline-step" style="border-color:var(--accent-cyan);"><strong>[T=20ms]</strong> Agent 1 unlocks <code>seat_mutex</code> 🔓.</div>
      <div class="timeline-step" style="border-color:var(--accent-indigo);"><strong>[T=21ms]</strong> Agent 2 unblocks ➔ Acquires <code>seat_mutex</code> 🔒.</div>
      <div class="timeline-step" style="border-color:var(--accent-amber);"><strong>[T=25ms]</strong> Agent 2 checks Seat 5 ➔ <span style="color:var(--accent-rose);">ALREADY RESERVED</span>.</div>
      <div class="timeline-step" style="border-color:var(--accent-rose);"><strong>[T=26ms]</strong> Agent 2 rejects Passenger 6 request safely.</div>
      <div class="timeline-step" style="border-color:var(--accent-cyan);"><strong>[T=30ms]</strong> Agent 2 unlocks <code>seat_mutex</code> 🔓.</div>
      <div class="timeline-step" style="background:rgba(16,185,129,0.15); border-color:var(--accent-emerald);">
        <strong style="color:var(--accent-emerald);">✓ RACE CONDITION PREVENTED! NO DUPLICATE BOOKING.</strong><br>
        Mutual exclusion ensured only one agent accessed Seat 5 at a time.
      </div>
    `;

    STATE.stats.raceConditionsPrevented++;
    renderStats();
  }

  // Event Listeners
  dom.btnAutoRun.addEventListener('click', () => {
    if (STATE.isRunningAuto) return;
    STATE.isRunningAuto = true;
    dom.btnAutoRun.disabled = true;
    dom.btnPause.disabled = false;
    STATE.autoTimer = setInterval(executeNextStep, STATE.speedMs);
  });

  dom.btnPause.addEventListener('click', () => {
    STATE.isRunningAuto = false;
    dom.btnAutoRun.disabled = false;
    dom.btnPause.disabled = true;
    clearInterval(STATE.autoTimer);
  });

  dom.btnNextStep.addEventListener('click', () => {
    if (STATE.isRunningAuto) {
      STATE.isRunningAuto = false;
      dom.btnAutoRun.disabled = false;
      dom.btnPause.disabled = true;
      clearInterval(STATE.autoTimer);
    }
    executeNextStep();
  });

  dom.btnReset.addEventListener('click', () => {
    STATE.isRunningAuto = false;
    dom.btnAutoRun.disabled = false;
    dom.btnPause.disabled = true;
    clearInterval(STATE.autoTimer);

    // Reset State
    STATE.passengers.forEach(p => p.status = 'WAITING');
    STATE.buffer = [];
    STATE.semaphores.empty = 5;
    STATE.semaphores.full = 0;
    STATE.mutexes.buffer_mutex = { locked: false, owner: null, waiting: [] };
    STATE.mutexes.seat_mutex = { locked: false, owner: null, waiting: [] };
    STATE.agents.forEach(a => { a.status = 'IDLE'; a.currentReq = null; });
    STATE.seats.forEach(s => { s.status = 'AVAILABLE'; s.assignedTo = null; s.lockedBy = null; });
    STATE.stats = {
      totalPassengers: 10,
      requestsGenerated: 0,
      requestsProcessed: 0,
      successfulReservations: 0,
      failedReservations: 0,
      availableSeats: 10,
      reservedSeats: 0,
      bufferUsage: '0 / 5',
      raceConditionsPrevented: 0,
      duplicateBookings: 0
    };
    STATE.stepQueue = generateStepQueue();
    STATE.currentStepIndex = 0;

    dom.stepBadge.textContent = 'READY';
    dom.stepDesc.textContent = 'Click "Start Auto" or "Next Step" to begin Producer–Consumer simulation.';
    dom.eventLogTerminal.innerHTML = '';
    dom.completionPanel.hidden = true;
    addLog('Simulation reset to initial state.', 'normal');

    renderPassengers();
    renderBuffer();
    renderAgents();
    renderSyncPanels();
    renderSeatGrid();
    renderStats();
    highlightWorkflowNode(null);
    highlightCodeLine(null);
  });

  dom.syncToggleBtn.addEventListener('click', () => {
    STATE.syncEnabled = !STATE.syncEnabled;
    if (STATE.syncEnabled) {
      dom.syncToggleBtn.className = 'btn btn-primary';
      dom.syncToggleBtn.innerHTML = '🛡️ Sync Enabled';
      addLog('Synchronization (Mutex & Semaphores) ENABLED.', 'success');
    } else {
      dom.syncToggleBtn.className = 'btn btn-danger';
      dom.syncToggleBtn.innerHTML = '⚠️ Sync Disabled (Race Chaos)';
      addLog('⚠️ Synchronization DISABLED! Race conditions may occur.', 'error');
    }
  });

  dom.speedBtns.forEach(btn => {
    btn.addEventListener('click', () => {
      dom.speedBtns.forEach(b => b.classList.remove('active'));
      btn.classList.add('active');
      STATE.speedMs = parseInt(btn.dataset.speed, 10);
      if (STATE.isRunningAuto) {
        clearInterval(STATE.autoTimer);
        STATE.autoTimer = setInterval(executeNextStep, STATE.speedMs);
      }
    });
  });

  dom.tabBtns.forEach(btn => {
    btn.addEventListener('click', () => {
      dom.tabBtns.forEach(b => b.classList.remove('active'));
      dom.tabContents.forEach(c => c.classList.remove('active'));
      btn.classList.add('active');
      const tabId = btn.dataset.tab;
      document.getElementById(`tab-${tabId}`).classList.add('active');
    });
  });

  dom.btnRaceDemo.addEventListener('click', () => {
    // Switch to race condition tab
    dom.tabBtns.forEach(b => b.classList.remove('active'));
    dom.tabContents.forEach(c => c.classList.remove('active'));
    const raceTabBtn = Array.from(dom.tabBtns).find(b => b.dataset.tab === 'race');
    if (raceTabBtn) raceTabBtn.classList.add('active');
    document.getElementById('tab-race').classList.add('active');
  });

  dom.btnRunRaceUnsync.addEventListener('click', runRaceDemoUnsynchronized);
  dom.btnRunRaceSync.addEventListener('click', runRaceDemoSynchronized);

  // Initial Initialization
  STATE.stepQueue = generateStepQueue();
  renderPassengers();
  renderBuffer();
  renderAgents();
  renderSyncPanels();
  renderSeatGrid();
  renderStats();
  addLog('OS Virtual Lab Initialized: Airport Seat Reservation Simulator ready.', 'normal');
});
